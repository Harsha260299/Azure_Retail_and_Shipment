# ASDA Retail Data Validation Pipeline

This project is a replica of my ASDA retail data engineering work, recreated with synthetic sample data for portfolio demonstration. It covers ingestion, validation, rejected-record handling and reporting without company data, credentials or internal systems.

ASDA is the business context. This is an independent portfolio demonstration, not an official ASDA system or a claim about its production architecture.

## Architecture
![ASDA retail validation architecture: S3 to ADF to ADLS landing to Databricks, branching to Azure SQL and rejected-data storage](https://raw.githubusercontent.com/Harsha260299/Azure_retail_mirror_pro/819cb7bf0ce2283781ea250c87a302fabd2830c4/docs/architecture.svg)

[Open the full-size architecture diagram](docs/architecture.svg)

The main flow is left to right. Approved records go to Azure SQL; rejected records go to a separate ADLS zone. Credential management, reference checks, metadata and rerun controls sit below the flow.

### File formats
| Stage | Orders | Order items |
|---|---|---|
| Amazon S3 source | CSV | JSON |
| ADF ingestion | Parse CSV and write Snappy Parquet | Copy JSON unchanged |
| ADLS landing / Databricks input | orders.parquet | order_items.json |
| Rejected orders | Snappy Parquet with rejection metadata | Raw items retained |

CSV remains only as the incoming source fixture. ADF performs a real format conversion; changing a filename alone does not create Parquet. The Node.js demo tests validation rules against the source fixture and does not execute the cloud conversion.

## Run locally
Node.js 20 or newer is required. No dependencies or cloud credentials are needed.
```bash
npm test
npm run demo
```
Results are written to `output/`: sales_reporting.json, rejected_orders.json and audit.json.

The fixture has **10 input rows, 4 accepted orders and 6 rejected rows**. Accepted order IDs: 1001, 1005, 1006, 1009. Combined basket value: **GBP 17.50**. This includes approved canceled orders and is not recognised revenue.

## Included assets
- `data/`: invented grocery orders, newline-delimited item JSON, synthetic customers and statuses.
- `notebooks/asda_retail_validation.py`: importable Databricks PySpark source notebook.
- `sql/setup.sql`: reference data and reporting schema.
- `adf/pipeline.json` and `adf/datasets/`: ADF authoring templates.
- `src/validation.mjs`, `scripts/demo.mjs`, `tests/validation.test.mjs`: local rule mirror, runner and seven tests.
- `tests/pipeline-contract.test.mjs`: CSV-to-Parquet mappings, notebook path alignment and ingestion dependencies.
- `docs/deployment.md`: configuration, verification and recovery guide.

## Validation and reporting
First failure wins: missing required ID/date/customer (1 sample row), repeated order ID (2 rows), blank/unsupported status (2 rows), unknown customer (1 row). Every occurrence of a duplicate ID is quarantined; no survivor is guessed. Status values are trimmed and uppercased.

Reporting has one row per accepted order. Item prices use integer pence and are aggregated before joining to avoid inflated totals. Duplicate or malformed items, orphan items and accepted orders without items fail the batch. Rejected orders retain reason, source path, timestamp and run ID. Items for rejected orders remain in raw storage. Date validation checks presence only.

The cloud notebook refreshes a dedicated demo SQL snapshot using JDBC overwrite/truncate. It is not an incremental upsert or an atomic transaction. A failed write may leave partial reporting data; preserve immutable inputs and rerun after correction. Never target a production table. ADF concurrency is 1; do not run the notebook concurrently elsewhere.

## Reference and attribution
- [Pushpak Vootla: Retail Data Engineering Project](https://github.com/PushpakVootla21/Retail_Data_Engineering_Project)
- [Retail Databricks Validation Pipeline case study](https://pushpakvootla.cloud/projects/retail-databricks-validation-pipeline#architecture)

This is a newly written implementation with new synthetic data. The reference notebook at commit `07abd8a0cf744af41888c624a404fbccd20aa384` rejects whole files; this version deliberately quarantines individual rows. The reference notebook reads item CSV, whereas this version uses JSON as described in the case study. No upstream screenshots, outputs or datasets are republished.

## Verification
All 10 local tests passed: seven validation-rule tests and three pipeline format/dependency contract checks. Azure/Databricks execution has not been verified in a live subscription. Cloud assets require linked services, permissions, secrets and resources described in the deployment guide. No production performance or ASDA operational results are claimed.
