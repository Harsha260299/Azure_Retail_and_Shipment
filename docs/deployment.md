# Cloud deployment and operations

These are ADF authoring templates and an importable Databricks notebook, not a provisioned environment. Local tests exercise the JavaScript rule mirror. PySpark and cloud integration must be verified in your own demo subscription.

## Configure the demo
1. Create a dedicated Azure resource group, ADLS Gen2 account/container, Azure SQL database, Key Vault, Databricks workspace/compute and Data Factory, plus an S3 bucket. Use a supported Databricks Runtime with the SQL Server JDBC driver available.
2. Run sql/setup.sql once in the empty demo database. It seeds customers and status values matching data/.
3. Upload data/orders.csv and data/order_items.json to S3 under asda-demo/. JSON is newline-delimited, one item per line.
4. Configure ADF linked services named ls_s3, ls_adls and ls_databricks. Use Key Vault secret references for required S3/Databricks credentials and prefer managed identity for ADLS. These services are environment-specific and are not provisioned by the templates.
5. Import both JSON files under adf/datasets/, then adf/pipeline.json. Validate the linked services before publishing. These are ADF authoring assets, not a complete ARM deployment.
6. Import notebooks/asda_retail_validation.py as a Python source notebook at /Shared/asda_retail_validation.
7. Configure Databricks ADLS access using an approved storage identity/external location. Configure a Key Vault-backed secret scope named asda-demo with sql-user and sql-password. Grant SELECT on reference tables and the permissions needed to refresh the dedicated reporting table. Never commit secret values.
8. Restrict network/firewall access to the services that require it and verify connectivity from ADF and Databricks compute.

## Pipeline parameters
| Parameter | Value |
|---|---|
| s3_bucket | Your bucket name |
| s3_prefix | asda-demo |
| adls_container | asda-demo |
| landing_folder | landing |
| landing_root | abfss://asda-demo@YOURACCOUNT.dfs.core.windows.net/landing |
| output_root | abfss://asda-demo@YOURACCOUNT.dfs.core.windows.net/processed |
| sql_server | YOURSERVER.database.windows.net |
| sql_database | asda_demo |
| secret_scope | asda-demo |
| notebook_path | /Shared/asda_retail_validation |

Replace all placeholders. landing_root must point to the exact account, container and landing_folder used by ls_adls and the dataset parameters. ADF appends its run ID to the copy destination; the notebook appends the same ID to landing_root. Keep output_root separate. Do not manually append the run ID to either root parameter.

## Verify a run
Trigger one pipeline run. Both copy activities must succeed before ValidateAndReport starts.
- SQL sales_reporting: four rows (1001, 1005, 1006, 1009); SUM(total_pence) = 1750.
- ADLS processed/RUN_ID/rejected_orders: six rows with reason codes and audit columns.
- ADLS processed/RUN_ID/audit: JSON text with ten input, four accepted and six rejected orders.
- Raw files remain under landing/RUN_ID.
- ADF notebook output contains the same audit summary.

Invalid item IDs, duplicate items, nonpositive quantities, negative prices, orphan items and accepted orders without items fail the batch. Rejected orders are preserved at row level; their items remain in raw storage. Required-date validation checks presence, not calendar semantics. Date format, store references and product catalogue validation are extensions.

## Reruns and failure recovery
Reporting is a full batch snapshot across all approved statuses, including canceled orders. It is not recognised revenue. ADF pipeline concurrency is 1; do not execute the notebook concurrently through another route.

The notebook uses JDBC overwrite with truncate against a dedicated demo table. Repeating the same batch replaces the snapshot rather than appending duplicates, but the multi-partition refresh is not atomic. Failure can leave partial or empty reporting data. Keep immutable inputs, investigate the failed activity and rerun to restore the snapshot. Audit is written after SQL, so an audit failure can occur after a successful SQL refresh.

Each new ADF run gets its own landing/quarantine directory. Re-executing the notebook with the same run ID replaces that run's quarantine/audit output and the SQL snapshot. Never target a production reporting table. Production extensions should include transactional SQL staging/promotion, business-key upserts, immutable run manifests and concurrency controls.

## Official references
- [ADF Amazon S3 connector](https://learn.microsoft.com/en-us/azure/data-factory/connector-amazon-simple-storage-service)
- [ADF Databricks notebook activity](https://learn.microsoft.com/en-us/azure/data-factory/transform-data-databricks-notebook)
- [ADF ADLS Gen2 connector](https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-data-lake-storage)
