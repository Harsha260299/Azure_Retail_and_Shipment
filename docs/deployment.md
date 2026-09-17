# Cloud deployment and operations

These are ADF authoring templates and an importable Databricks notebook, not a provisioned environment. Local tests exercise the JavaScript rule mirror. PySpark and cloud integration must be verified in your own demo subscription.

## Configure the demo
1. Create a dedicated Azure resource group, ADLS Gen2 account/container, Azure SQL database, Key Vault, Databricks workspace/compute and Data Factory, plus an S3 bucket. Use a supported Databricks Runtime with the SQL Server JDBC driver available.
2. Run sql/setup.sql and then sql/002_shipments.sql in the empty demo database. For an existing demo database, run only sql/002_shipments.sql to add shipment tables. It seeds customers and status values matching data/.
3. Upload data/orders.csv, data/order_items.json and data/shipments.json to S3 under asda-demo/. CSV is the source contract, not the lake format. ADF parses orders.csv and writes orders.parquet with Snappy compression in ADLS. Items and shipments remain newline-delimited JSON and are copied unchanged.
4. Configure ADF linked services named ls_s3, ls_adls and ls_databricks. Use Key Vault secret references for required S3/Databricks credentials and prefer managed identity for ADLS. These services are environment-specific and are not provisioned by the templates.
5. Import all four JSON files under adf/datasets/, then adf/pipeline.json. Validate the linked services before publishing. These are ADF authoring assets, not a complete ARM deployment.
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
Trigger one pipeline run. All three copy activities (CopyOrders, CopyOrderItems and CopyShipments) must succeed before ValidateAndReport starts.
- SQL sales_reporting: four rows (1001, 1005, 1006, 1009); SUM(total_pence) = 1750.
- SQL shipment_reporting: four rows (ASDA-CN-001 through ASDA-CN-004). Two consignments belong to order 1001.
- ADLS processed/RUN_ID/rejected_shipments: Parquet with five rejected records and reason codes.
- Audit shipments section: nine input, four accepted, five rejected.
- ADLS processed/RUN_ID/rejected_orders: Parquet part files with six rows, reason codes and audit columns.
- ADLS processed/RUN_ID/audit: JSON text with ten input, four accepted and six rejected orders.
- ADLS landing/RUN_ID contains orders.parquet, order_items.json and shipments.json. The original CSV remains in S3; it is not copied unchanged to ADLS. Preserve/version the S3 input for exact source replay.
- ADF notebook output contains the same audit summary.

Invalid item IDs, duplicate items, nonpositive quantities, negative prices, orphan items and accepted orders without items fail the batch. Rejected orders are preserved at row level; their items remain in raw storage. Required-date validation checks presence, not calendar semantics. Date format, store references and product catalogue validation are extensions.

## Parquet contract
The CopyOrders activity uses DelimitedTextSource and ParquetSink, with explicit string mappings for order_id, order_date, customer_id, order_status and store_id. Keeping identifiers as strings preserves their text representation and lets the validation stage handle blanks. The landing dataset uses Snappy compression. Do not rename a CSV file to .parquet: ADF must serialize the records into Parquet. CopyOrderItems and CopyShipments use BinarySource/BinarySink for their JSON files.

The notebook reads landing/RUN_ID/orders.parquet and records that path in source_file. Rejected orders are written as a Parquet directory; audit remains JSON text. The Node demo continues to read the CSV fixture and tests validation rules only. It does not test ADF execution or Parquet serialization. Validate both in a demo Azure run before deployment.

After migration, run the updated pipeline to create a new landing batch. Existing landing folders containing orders.csv are not compatible with the new notebook.

## Reruns and failure recovery
Reporting is a full batch snapshot across all approved statuses, including canceled orders. It is not recognised revenue. ADF pipeline concurrency is 1; do not execute the notebook concurrently through another route.

The notebook uses JDBC overwrite with truncate against the dedicated sales_reporting and shipment_reporting demo tables. These are separate writes, not a transaction across tables. A failure between them can leave different snapshot versions; rerun the whole immutable batch to restore consistency. Repeating the same batch replaces the snapshot rather than appending duplicates, but the multi-partition refresh is not atomic. Failure can leave partial or empty reporting data. Keep immutable inputs, investigate the failed activity and rerun to restore the snapshot. Audit is written after SQL, so an audit failure can occur after a successful SQL refresh.

Each new ADF run gets its own landing/quarantine directory. Re-executing the notebook with the same run ID replaces that run's quarantine/audit output and the SQL snapshot. Never target a production reporting table. Production extensions should include transactional SQL staging/promotion, business-key upserts, immutable run manifests and concurrency controls.

## Shipment data contract
Shipment input is a snapshot with one record per consignment_id, not a tracking-event stream. Required fields are consignment_id, order_id and carrier; shipment_status must match dbo.valid_shipment_status. Service level, tracking reference, dispatched_at, expected_delivery_date and delivered_at are retained as optional source text. Dates use ISO examples but parsing, chronology and delivery SLA validation are not implemented.

Duplicate consignment IDs are all quarantined. Unknown orders and orders rejected by order validation have different reason codes. Accepted consignments are written separately from sales_reporting so split shipments do not multiply basket values. Orders without a shipment remain valid, including pending/canceled orders. No physical SQL foreign key is created because these demo snapshots are independently truncated; the notebook checks the accepted-order relationship before writing.

Run sql/002_shipments.sql before the updated pipeline and upload shipments.json to S3. An older landing batch without shipments.json cannot run through the new notebook. No carrier API calls or real tracking data are involved.

## Official references
- [ADF Parquet format and sink settings](https://learn.microsoft.com/en-us/azure/data-factory/format-parquet)
- [ADF delimited-text format](https://learn.microsoft.com/en-us/azure/data-factory/format-delimited-text)
- [ADF Amazon S3 connector](https://learn.microsoft.com/en-us/azure/data-factory/connector-amazon-simple-storage-service)
- [ADF Databricks notebook activity](https://learn.microsoft.com/en-us/azure/data-factory/transform-data-databricks-notebook)
- [ADF ADLS Gen2 connector](https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-data-lake-storage)
