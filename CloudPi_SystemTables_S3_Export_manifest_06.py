# Databricks notebook source
# ============================================================================
# CloudPi - Databricks system-tables -> S3 export job
# ----------------------------------------------------------------------------
# Run INSIDE Databricks: attach to a cluster/serverless, then schedule as a
# daily Job. This is the only script that runs inside Databricks - CloudPi
# never calls Databricks/AWS APIs; it only reads what this writes to S3.
#
# Behavior: configured via widgets with input validation; tables are exported
# concurrently; each write is retried on transient errors; every table is
# required (a read failure fails the run); a JSON run summary and a _SUCCESS
# marker are written. Serverless-safe: no .cache()/.persist() and no global
# spark.conf partitionOverwriteMode (uses a per-write .option()).
#
# Design (do NOT change without reading the collector):
# - Export RAW tables as-is. Parquet preserves nested structs and the
#   custom_tags MAP. DO NOT flatten here.
# - billing.usage is MONTH-PARTITIONED (year=/month=), one parquet per month
#   (matches how the other CSPs store billing); everything else is an OVERWRITE
#   SNAPSHOT filtered by a recency cutoff (collector reads the dir).
# - REQUIRED vs OPTIONAL is driven by what the CloudPi collector actually reads.
#   Billing/metric tables CloudPi ingests (billing_usage, list_prices, clusters,
#   warehouses, jobs, workspaces, node_timeline, warehouse_events) are REQUIRED —
#   a read failure fails the run (loud, retryable). Tables CloudPi does NOT ingest
#   (query.history, job_run_timeline) are OPTIONAL: they are emitted best-effort
#   so a transient read failure (e.g. a Delta-Sharing JsonParseException on the
#   high-volume query.history telemetry) is SKIPPED, not fatal — it must never
#   block the billing/metric data CloudPi depends on. pipelines is also best-effort:
#   the collector only uses it for friendly name lookups and degrades to the raw id.
# - Path layout MUST be:
#   {BUCKET}/org={ORG}/cloud={CLOUD}/source=system_tables/table={name}/[year=/month=]
# - Snapshot tables may write MULTIPLE files (large tables aren't coalesced);
#   the collector reads ALL parquet files in the snapshot dir.
# ============================================================================
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any
from collections.abc import Callable

from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, lit, expr, coalesce, broadcast
from pyspark.sql.functions import count as _spark_count, max as _spark_max, min as _spark_min

# COMMAND ----------

# ---- Parameters (Job widgets; safe defaults for a manual run) ----
dbutils.widgets.text("bucket", "s3://databricks-billing-cloudpi", "S3 bucket (s3://...)")
dbutils.widgets.text("org", "1", "Org id")
dbutils.widgets.text("cloud", "aws", "Underlying cloud (aws|azure|gcp)")
dbutils.widgets.text("lookback", "3", "Billing lookback days (informational; monthly files re-export current+previous month)")
dbutils.widgets.text("metric_lookback", "30", "Metric/event lookback days")
dbutils.widgets.text("backfill_months", "0", "Full-month backfill (0 = daily; N = last N whole months)")
dbutils.widgets.text("correction_lookback", "7", "Re-pull days with billing corrections ingested in last N days (0=off)")
# Optional per-workspace region overrides for MULTI-REGION accounts (workspaces in
# different regions under one billing account). JSON {workspace_url: region}, e.g.
# {"https://dbc-xxxx.cloud.databricks.com":"us-west-2"}. Leave "{}" for single-region
# accounts — region then derives from current_metastore(). Implements the Databricks
# FOCUS reference's "for multi-region deployments, map workspace_url to regions".
dbutils.widgets.text("workspace_region_map", "{}", "Per-workspace region overrides JSON {workspace_url: region} (multi-region only)")

BUCKET: str = dbutils.widgets.get("bucket").strip().rstrip("/")
ORG: str = dbutils.widgets.get("org").strip()
CLOUD: str = dbutils.widgets.get("cloud").strip().lower()
LOOKBACK: int = int(dbutils.widgets.get("lookback").strip())
METRIC_LOOKBACK: int = int(dbutils.widgets.get("metric_lookback").strip())
BACKFILL_MONTHS: int = int(dbutils.widgets.get("backfill_months").strip() or "0")
CORRECTION_LOOKBACK: int = int(dbutils.widgets.get("correction_lookback").strip() or "0")
import json as _json
try:
    WS_REGION_MAP: dict = _json.loads(dbutils.widgets.get("workspace_region_map").strip() or "{}")
    if not isinstance(WS_REGION_MAP, dict):
        WS_REGION_MAP = {}
except Exception:
    WS_REGION_MAP = {}

# ---- Validate inputs early; fail fast with a clear message ----
assert BUCKET.startswith("s3://"), f"bucket must start with s3:// (got: {BUCKET})"
assert ORG, "org id must not be empty"
assert CLOUD in ("aws", "azure", "gcp"), f"cloud must be aws|azure|gcp (got: {CLOUD})"
assert LOOKBACK >= 1, f"lookback must be >= 1 (got: {LOOKBACK})"
assert METRIC_LOOKBACK >= 1, f"metric_lookback must be >= 1 (got: {METRIC_LOOKBACK})"
assert BACKFILL_MONTHS >= 0, f"backfill_months must be >= 0 (got: {BACKFILL_MONTHS})"
assert CORRECTION_LOOKBACK >= 0, f"correction_lookback must be >= 0 (got: {CORRECTION_LOOKBACK})"

BASE: str = f"{BUCKET}/org={ORG}/cloud={CLOUD}/source=system_tables"
RUN_TS: str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
# UTC-pinned cutoffs as date literals (avoid session-timezone drift).
_today_utc = datetime.now(timezone.utc).date()


def _latest_exported_billing_month():
    """First-day of the newest MONTH already present in the billing_usage export.

    Billing is written one parquet **per month** (``year=/month=``), so the
    watermark is month-grained. Scans the existing ``year=/month=`` partitions
    under BASE and returns the first day of the newest exported month (a
    ``date``), or ``None`` on the first run / if the export tree can't be listed.
    Drives the daily cutoff so each run resumes from the last exported month,
    making the schedule self-healing across job downtime (no missed-month gap).
    Silent on failure so first runs fall back to the plain monthly window.
    """
    best = None
    try:
        years = dbutils.fs.ls(f"{BASE}/table=billing_usage/")
    except Exception:  # noqa: BLE001 - first run: nothing exported yet
        return None
    for y in years:
        if "year=" not in y.name:
            continue
        try:
            months = dbutils.fs.ls(y.path)
        except Exception:  # noqa: BLE001
            continue
        for m in months:
            if "month=" not in m.name:
                continue
            try:
                cur = datetime(
                    int(y.name.split("year=")[-1].strip("/")),
                    int(m.name.split("month=")[-1].strip("/")),
                    1,
                    tzinfo=timezone.utc,
                ).date()
            except ValueError:
                continue
            if best is None or cur > best:
                best = cur
    return best


# Billing cutoff (always a MONTH boundary — billing is written one file/month):
#   backfill_months > 0  -> snap to the FIRST day of the month (backfill_months-1)
#                           months back, so "last N months" = N WHOLE calendar
#                           months (e.g. run in June with 4 -> from March 1).
#   backfill_months == 0 -> DAILY watermark: re-export the CURRENT + PREVIOUS
#                           whole month (the previous month catches late
#                           finalization that lands after month-end). If the job
#                           was down for months, resume from the last EXPORTED
#                           month instead, so the gap is re-exported (self-heal).
#                           Falls back to current+previous month on the first run
#                           / unreadable export tree. Re-exported months never
#                           double-count — CloudPi replaces the whole month in one
#                           delete+insert. `lookback` (days) no longer drives the
#                           billing cutoff under monthly files; it is retained for
#                           the widget contract and snapshot/metric windows.
def _month_start_back(n: int):
    """First day of the month `n` whole months before the current UTC month."""
    _idx = _today_utc.year * 12 + (_today_utc.month - 1) - n
    return datetime(_idx // 12, _idx % 12 + 1, 1, tzinfo=timezone.utc).date()


if BACKFILL_MONTHS > 0:
    _start = _month_start_back(BACKFILL_MONTHS - 1)
    BILLING_CUTOFF: str = _start.isoformat()
    _cutoff_mode = f"backfill({BACKFILL_MONTHS}mo)"
else:
    _prev_month = _month_start_back(1)
    _watermark = _latest_exported_billing_month()
    _anchor = min(_prev_month, _watermark) if _watermark is not None else _prev_month
    BILLING_CUTOFF: str = _anchor.isoformat()
    _cutoff_mode = (
        f"watermark({_watermark.isoformat()})" if _watermark is not None else "first-run(prev+current month)"
    )
METRIC_CUTOFF: str = (_today_utc - timedelta(days=METRIC_LOOKBACK)).isoformat()

_log_lock = Lock()


def log(msg: str) -> None:
    """Thread-safe timestamped print (parallel exports share stdout)."""
    with _log_lock:
        print(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {msg}")


# COMMAND ----------

# ---- Export helpers (serverless-safe: no .cache(), no global spark.conf) ----
WRITE_RETRIES: int = 3


def _write_with_retry(do_write: Callable[[str], None], path: str) -> None:
    """Write to S3, retrying transient failures with a small linear backoff.

    Args:
        do_write: Callable that performs the actual write given the target path.
        path: Destination S3 path (for logging).

    Raises:
        Exception: the last error if all attempts fail.
    """
    last: Exception | None = None
    for attempt in range(1, WRITE_RETRIES + 1):
        try:
            do_write(path)
            return
        except Exception as exc:  # noqa: BLE001 - retry any transient write error
            last = exc
            log(f"[retry {attempt}/{WRITE_RETRIES}] write {path}: {type(exc).__name__}: {str(exc)[:80]}")
            time.sleep(2 * attempt)
    assert last is not None
    raise last


def _write_manifests(df: DataFrame, out: str, date_col: str) -> None:
    """Write one ``manifest.json`` per (year, month) touched, AFTER the parquet.

    Mirrors the AWS export ``Manifest.json``: an authoritative per-billing-period
    file list stamped with a unique ``assemblyId``. CloudPi reads ``dataFiles[]``
    and re-ingests a month only when the ``assemblyId`` changes — so it never
    relies on (Spark-renamed) part-file paths, and one daily run that rewrites a
    month only flips the assemblyId of the month(s) it touched. Written LAST so
    the manifest's presence means the month's parquet is complete.

    Billing is written one parquet **per month**, so ``dataFiles`` has a single
    entry per month listing ALL part-file(s) Spark emitted for it (usually one).
    ``date_range`` is the actual min/max of ``date_col`` in the month, so CloudPi
    freshness reflects the real data span, not the calendar month.
    """
    agg = (
        df.groupBy("year", "month")
        .agg(
            _spark_count(lit(1)).alias("row_count"),
            _spark_min(col(date_col)).alias("dmin"),
            _spark_max(col(date_col)).alias("dmax"),
        )
        .collect()
    )
    for r in agg:
        y, m = r["year"], r["month"]
        ym = f"{y}-{m}"
        month_dir = f"{BASE}/table={out}/year={y}/month={m}/"
        try:
            paths = [f.path for f in dbutils.fs.ls(month_dir) if f.name.endswith(".parquet")]
        except Exception as exc:  # noqa: BLE001 - a missing dir just yields no files
            log(f"[manifest] could not list {month_dir}: {type(exc).__name__}")
            paths = []
        manifest: dict[str, Any] = {
            "assemblyId": str(uuid.uuid4()),
            "billing_period": ym,
            "table": out,
            "run_ts": RUN_TS,
            "lookback_days": LOOKBACK,
            "date_range": {"from": str(r["dmin"])[:10], "to": str(r["dmax"])[:10]},
            "row_count": r["row_count"],
            "dataFiles": [{"month": ym, "paths": paths, "row_count": r["row_count"]}],
        }
        path = f"{BASE}/table={out}/year={y}/month={m}/manifest.json"
        _write_with_retry(
            lambda p, _mf=manifest: dbutils.fs.put(p, json.dumps(_mf), overwrite=True),
            path,
        )
        log(
            f"[manifest] {out} {ym}: 1 monthly file ({len(paths)} part(s)), "
            f"{manifest['row_count']:,} rows, asm={manifest['assemblyId'][:8]}"
        )


def _retracted_months(df: DataFrame, out: str, cutoff: str) -> list[str]:
    """Months IN the export window that have an existing manifest but ZERO rows now.

    A full-month retraction (Databricks restates a month down to nothing) leaves
    a stale manifest on storage whose assemblyId never changes — so CloudPi keeps
    the old rows forever. We detect such months by listing the existing
    ``year=/month=`` manifest tree and subtracting the months still present in the
    current export ``df``. Only months at/after the billing cutoff are considered
    (older months are out of the re-export window and intentionally untouched).
    Returns a list of 'YYYY-MM' strings. Serverless-safe (dbutils.fs.ls only).
    """
    cutoff_ym = cutoff[:7]  # 'YYYY-MM-DD' -> 'YYYY-MM'
    present = {
        f"{r['year']}-{r['month']}"
        for r in df.select("year", "month").distinct().collect()
    }
    existing: set[str] = set()
    try:
        years = dbutils.fs.ls(f"{BASE}/table={out}/")
    except Exception as exc:  # noqa: BLE001 - first run / no tree yet
        log(f"[manifest] could not list {BASE}/table={out}/ for retraction scan: {type(exc).__name__}")
        return []
    for y in years:
        if "year=" not in y.name:
            continue
        try:
            months = dbutils.fs.ls(y.path)
        except Exception:  # noqa: BLE001
            continue
        yy = y.name.split("year=")[-1].strip("/")
        for m in months:
            if "month=" not in m.name:
                continue
            mm = m.name.split("month=")[-1].strip("/")
            ym = f"{yy}-{mm}"
            try:
                has_manifest = any(f.name == "manifest.json" for f in dbutils.fs.ls(m.path))
            except Exception:  # noqa: BLE001
                has_manifest = False
            if has_manifest:
                existing.add(ym)
    # Retracted = had a manifest, is in-window (>= cutoff month), now has no rows.
    return sorted(ym for ym in existing if ym >= cutoff_ym and ym not in present)


def _write_empty_manifest(out: str, ym: str) -> None:
    """Write a fresh-assemblyId, zero-row, empty-dataFiles manifest for a retracted
    month so CloudPi sees a CHANGE and clears that month's rows (delete-only)."""
    y, m = ym.split("-")
    manifest: dict[str, Any] = {
        "assemblyId": str(uuid.uuid4()),
        "billing_period": ym,
        "table": out,
        "run_ts": RUN_TS,
        "lookback_days": LOOKBACK,
        "date_range": {"from": f"{ym}-01", "to": f"{ym}-01"},
        "row_count": 0,
        "dataFiles": [],
    }
    path = f"{BASE}/table={out}/year={y}/month={m}/manifest.json"
    _write_with_retry(
        lambda p, _mf=manifest: dbutils.fs.put(p, json.dumps(_mf), overwrite=True),
        path,
    )
    log(f"[manifest] {out} {ym}: RETRACTED (0 rows), asm={manifest['assemblyId'][:8]}")


def _attach_region(frame: DataFrame) -> DataFrame:
    """Add a per-row ``region`` column.

    Default (single-region account): region = split(current_metastore(), ':')[1] —
    the metastore's region, per the Databricks FOCUS reference.

    Multi-region account (workspaces in different regions under one billing account):
    when ``workspace_region_map`` is provided, resolve each row's workspace_url from
    system.access.workspaces_latest (joined on workspace_id), override the region from
    the map, and fall back to the metastore region for any workspace not in the map.
    This is the FOCUS reference's "for multi-region deployments, map workspace_url to
    regions". Both joins are on unique keys (left, broadcast) so no row fan-out.
    """
    meta_region = expr("split(current_metastore(), ':')[1]")
    if not WS_REGION_MAP or "workspace_id" not in frame.columns:
        return frame.withColumn("region", meta_region)
    ws = spark.table("system.access.workspaces_latest").select("workspace_id", "workspace_url")
    rmap = spark.createDataFrame(
        [(str(u), str(r)) for u, r in WS_REGION_MAP.items()],
        ["workspace_url", "_map_region"],
    )
    return (
        frame.join(broadcast(ws), "workspace_id", "left")
        .join(broadcast(rmap), "workspace_url", "left")
        .withColumn("region", coalesce(col("_map_region"), meta_region))
        .drop("workspace_url", "_map_region")
    )


def export_by_date(table: str, out: str, date_col: str, cutoff: str) -> int:
    """Export a date-partitioned billing table as ONE parquet per month.

    Billing is written one file per month (``year=/month=``) to match how the
    other CSPs store billing. `partitionOverwriteMode=dynamic` is a per-write
    option so only touched month-partitions are rewritten (serverless blocks the
    global spark.conf), leaving untouched months intact.

    Args:
        table: Source system table (e.g. ``system.billing.usage``).
        out: Output ``table=<out>`` folder name.
        date_col: Column used for the date filter and year/month partitions.
        cutoff: Inclusive ISO date lower bound (``YYYY-MM-DD``), a month boundary.

    Returns:
        Number of rows written.
    """
    src: DataFrame = spark.table(table)
    selected: DataFrame = src.filter(col(date_col) >= lit(cutoff))

    # Late-correction capture: Databricks restates billing via retraction +
    # restatement records that carry the ORIGINAL (old) usage_date but a RECENT
    # ingestion_date. Because billing is now written one partition PER MONTH and
    # the overwrite is dynamic, we must re-pull the WHOLE corrected MONTH (not
    # just the corrected days) — otherwise dynamic-overwrite would replace that
    # month's partition with only the correction rows and wipe the rest of the
    # month. Match on a yyyy-MM month key so every row of any month that had a
    # record ingested within the window is rewritten to its CURRENT state. Done
    # as a left-semi JOIN (not collect()+isin) so a large correction set never
    # lands on the driver; the < cutoff filter keeps it disjoint from `selected`
    # (which already holds the complete months at/after the cutoff), so the union
    # needs no dedup.
    if CORRECTION_LOOKBACK > 0 and "ingestion_date" in src.columns:
        _corr_cutoff = (_today_utc - timedelta(days=CORRECTION_LOOKBACK)).isoformat()
        _month_key = date_format(col(date_col), "yyyy-MM")
        corrected_months = (
            src.withColumn("__mk", _month_key)
            .filter(col("ingestion_date") >= lit(_corr_cutoff))
            .filter(col(date_col) < lit(cutoff))
            .select("__mk")
            .distinct()
        )
        corr_rows = (
            src.withColumn("__mk", _month_key)
            .join(corrected_months, on="__mk", how="left_semi")
            .drop("__mk")
        )
        selected = selected.unionByName(corr_rows)
        log(f"[corrections] {out}: also re-pulling full months with records ingested >= {_corr_cutoff}")

    # Region: billing.usage has no region column. Derive it as the Databricks FOCUS
    # reference does — split(current_metastore(), ':')[1] (the metastore region) — with
    # optional per-workspace overrides for multi-region accounts. See _attach_region
    # and the workspace_region_map widget.
    df: DataFrame = (
        _attach_region(selected)
        .withColumn("year", date_format(col(date_col), "yyyy"))
        .withColumn("month", date_format(col(date_col), "MM"))
        .repartition("year", "month")
    )
    n: int = df.count()
    _write_with_retry(
        lambda p: (
            df.write.option("partitionOverwriteMode", "dynamic")
            .partitionBy("year", "month")
            .mode("overwrite")
            .parquet(p)
        ),
        f"{BASE}/table={out}/",
    )
    log(f"[date] {table} -> table={out} {n:,} rows (>= {cutoff})")
    _write_manifests(df, out, date_col)
    # Full-month retraction: any in-window month that previously had a manifest
    # but has no rows in this export is published as an empty manifest (fresh
    # assemblyId, dataFiles=[]) so CloudPi clears that month instead of keeping
    # stale rows behind an unchanging assemblyId.
    for ym in _retracted_months(df, out, cutoff):
        _write_empty_manifest(out, ym)
    return n


def export_snapshot(
    table: str,
    out: str,
    ts_col: str | None = None,
    cutoff: str | None = None,
    coalesce_one: bool = True,
) -> int:
    """Export a full overwrite snapshot (collector reads ALL files in the dir).

    `coalesce(1)` keeps small-table commits cheap; large tables (node_timeline)
    write many files instead (``coalesce_one=False``) to avoid a single-task OOM.

    Args:
        table: Source system table.
        out: Output ``table=<out>`` folder name.
        ts_col: Optional timestamp column to filter recent rows by ``cutoff``.
        cutoff: Inclusive ISO date lower bound; applied only when ``ts_col`` is set.
        coalesce_one: Coalesce to a single file (cheap) vs many files (large tables).

    Returns:
        Number of rows written.
    """
    df: DataFrame = spark.table(table)
    if ts_col and cutoff:
        df = df.filter(col(ts_col) >= lit(cutoff))
    df = df.coalesce(1) if coalesce_one else df.repartition(8)
    n: int = df.count()
    _write_with_retry(lambda p: df.write.mode("overwrite").parquet(p), f"{BASE}/table={out}/")
    label = f"snap<{cutoff}" if (ts_col and cutoff) else "snapshot"
    log(f"[{label}] {table} -> table={out} {n:,} rows")
    return n


# COMMAND ----------

# ---- Run plan: each task is a dict so per-table options stay explicit.
#      `required` mirrors what the CloudPi collector consumes (see header):
#      collector-read tables are required (fail loud); query_history and
#      job_run_timeline are NOT ingested by CloudPi, so they are best-effort
#      (required=False) — a transient read failure is skipped, never fatal. ----
PLAN: list[dict[str, Any]] = [
    {"fn": export_by_date, "table": "system.billing.usage", "out": "billing_usage",
     "date_col": "usage_date", "cutoff": BILLING_CUTOFF, "required": True},
    {"fn": export_snapshot, "table": "system.billing.list_prices", "out": "list_prices",
     "required": True},
    {"fn": export_snapshot, "table": "system.compute.clusters", "out": "clusters",
     "required": True},
    {"fn": export_snapshot, "table": "system.compute.warehouses", "out": "warehouses",
     "required": True},
    {"fn": export_snapshot, "table": "system.lakeflow.jobs", "out": "jobs",
     "required": True},
    # pipelines: read by the collector's _load_name_lookups for friendly
    # resource names (pipeline_id -> name). Collector degrades to the raw id when
    # absent, so this is best-effort — never fail billing over a name lookup.
    {"fn": export_snapshot, "table": "system.lakeflow.pipelines", "out": "pipelines",
     "required": False},
    {"fn": export_snapshot, "table": "system.access.workspaces_latest", "out": "workspaces",
     "required": True},
    {"fn": export_snapshot, "table": "system.compute.node_timeline", "out": "node_timeline",
     "ts_col": "start_time", "cutoff": METRIC_CUTOFF, "coalesce_one": False, "required": True},
    {"fn": export_snapshot, "table": "system.compute.warehouse_events", "out": "warehouse_events",
     "ts_col": "event_time", "cutoff": METRIC_CUTOFF, "required": True},
    # job_run_timeline + query.history are NOT consumed by CloudPi — best-effort.
    {"fn": export_snapshot, "table": "system.lakeflow.job_run_timeline", "out": "job_run_timeline",
     "ts_col": "period_start_time", "cutoff": METRIC_CUTOFF, "required": False},
    {"fn": export_snapshot, "table": "system.query.history", "out": "query_history",
     "ts_col": "start_time", "cutoff": METRIC_CUTOFF, "coalesce_one": False, "required": False},
]

log(f"Run {RUN_TS} exporting system tables -> {BASE}")
log(f"Billing cutoff = {BILLING_CUTOFF} [{_cutoff_mode}]; metric cutoff = {METRIC_CUTOFF}")

# Thread-safe result collection (the tables are independent -> run in parallel).
results: dict[str, list[dict[str, Any]]] = {"ok": [], "skipped": [], "failed": []}
fatal_errors: list[str] = []
_results_lock = Lock()


def run_one(task: dict[str, Any]) -> None:
    """Run a single export task and record its outcome (thread-safe)."""
    fn, table, out, required = task["fn"], task["table"], task["out"], task["required"]
    try:
        if fn is export_by_date:
            rows = fn(table, out, task["date_col"], task["cutoff"])
        else:
            rows = fn(
                table, out,
                ts_col=task.get("ts_col"),
                cutoff=task.get("cutoff"),
                coalesce_one=task.get("coalesce_one", True),
            )
        with _results_lock:
            results["ok"].append({"table": table, "rows": rows})
    except Exception as exc:  # noqa: BLE001 - record per-table failure, decide fatality below
        msg = f"{type(exc).__name__}: {str(exc)[:120]}"
        with _results_lock:
            if required:
                log(f"[FAIL] {table} -> REQUIRED export failed: {msg}")
                results["failed"].append({"table": table, "error": msg})
                fatal_errors.append(f"required table {table} failed: {msg}")
            else:
                log(f"[SKIP] {table} -> {msg}")
                results["skipped"].append({"table": table, "error": msg})


with ThreadPoolExecutor(max_workers=len(PLAN)) as pool:
    list(pool.map(run_one, PLAN))

fatal: str | None = fatal_errors[0] if fatal_errors else None

# COMMAND ----------

# ---- Finalize: marker file, summary, and exit status ----
summary: dict[str, Any] = {
    "run_ts": RUN_TS, "bucket": BUCKET, "org": ORG, "cloud": CLOUD,
    "lookback": LOOKBACK, "metric_lookback": METRIC_LOOKBACK,
    "backfill_months": BACKFILL_MONTHS, "correction_lookback": CORRECTION_LOOKBACK,
    "billing_cutoff": BILLING_CUTOFF, "cutoff_mode": _cutoff_mode, "base": BASE,
    "exported": results["ok"], "skipped": results["skipped"], "failed": results["failed"],
    "status": "FAILED" if fatal else "SUCCESS",
}

# Single overwriting _SUCCESS marker (latest clean run) so markers don't accumulate.
if not fatal:
    try:
        dbutils.fs.put(f"{BASE}/_SUCCESS", f"{RUN_TS}\n{json.dumps(summary)}", overwrite=True)
        log(f"wrote marker {BASE}/_SUCCESS")
    except Exception as exc:  # noqa: BLE001 - marker is best-effort, never fail the run on it
        log(f"[WARN] could not write _SUCCESS marker: {type(exc).__name__}: {exc}")

log(
    f"Done. status={summary['status']} ok={len(results['ok'])} "
    f"skipped={len(results['skipped'])} failed={len(results['failed'])}"
)

if fatal:
    # Raising marks the Databricks Job run FAILED so alerts/retries fire.
    raise RuntimeError(f"Export failed: {fatal} | summary={json.dumps(summary)}")

dbutils.notebook.exit(json.dumps(summary))
