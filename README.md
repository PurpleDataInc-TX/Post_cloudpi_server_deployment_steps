# Post-CloudPi Server Deployment Steps

Customer-facing setup and integration guides to complete **after** the CloudPi server is deployed.

## Recommended order
1. `CloudPi_01_HTTPS_Requirements.docx` — HTTPS / TLS setup
2. `CloudPi_02_Email_Integration_and_User_Login.docx` — SMTP email, user invitation & login
   - `CloudPi_02b_SSO_Configuration.docx` — SSO (Azure Entra ID / Auth0)
3. `CloudPi_03_Ticketing_Integration.docx` — Jira / Azure DevOps / ServiceNow
4. `CloudPi_04_MSA_Creation.docx` — Master Service Account (access keys + keyless)
5. `CloudPi_05_AWS_Integration.docx` — AWS billing (CUR / FOCUS) onboarding
6. `CloudPi_06_Databricks_Billing_Daily_Jobs.docx` — Databricks billing export (daily job)
7. `CloudPi_07_Databricks_Metrics_Recommendations.docx` — Databricks metrics for recommendations

## Reference
- `CloudPi Databricks Billing Collection explanation.docx` — engineering overview of the Databricks → S3 → CloudPi architecture.
- `CloudPi_SystemTables_S3_Export_manifest_06.py` — the Databricks export notebook (exports 11 Unity Catalog system tables to S3: 8 required + 3 best-effort).
