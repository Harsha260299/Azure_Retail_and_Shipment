# Retail Data Validation Pipeline

This project is a replica of my ASDA retail data engineering work, recreated with synthetic sample data for portfolio demonstration. It covers ingestion, validation, rejected-record handling and reporting without company data, credentials or internal systems.

ASDA is the business context. This is an independent portfolio demonstration, not an official ASDA system or a claim about its production architecture.

## Architecture
![Retail validation architecture: S3 to ADF to ADLS landing to Databricks, branching to Azure SQL and rejected-data storage](https://raw.githubusercontent.com/Harsha260299/Azure_retail_mirror_pro/5b69651ece42d72bfc0ad38889cf85525d7ba9d1/docs/architecture.svg)

[Open the full-size architecture diagram](docs/architecture.svg)

The main flow is left to right. Approved records go to Azure SQL; rejected records go to a separate ADLS zone. Credential management, reference checks, metadata and rerun controls sit below the flow.

### File formats
| Stage | Orders | Order items and shipments |
|---|---|---|
| Amazon S3 source | CSV | JSON |
| ADF ingestion | Parse CSV and write Snappy Parquet | Copy JSON unchanged |
| ADLS landing / Databricks input | orders.parquet | order_items.json and shipments.json |
| Rejected records | Orders: Snappy Parquet | Shipments: Snappy Parquet; raw items retained |

CSV remains only as the incoming source fixture. ADF performs a real format conversion; changing a filename alone does not create Parquet. The Node.js demo tests validation rules against the source fixture and does not execute the cloud conversion.

## Run locally
Node.js 20 or newer is required. No dependencies or cloud credentials are needed.
```bash
npm test
npm run demo
```
Results are written to `output/`: sales_reporting.json, rejected_orders.json, shipment_reporting.json, rejected_shipments.json and audit.json.

The fixture has **10 input rows, 4 accepted orders and 6 rejected rows**. Accepted order IDs: 1001, 1005, 1006, 1009. Combined basket value: **GBP 17.50**. This includes approved canceled orders and is not recognised revenue.

## Shipment and consignment data
The project now includes nine synthetic shipment records: **four accepted consignments and five rejected records**. Order 1001 has two consignments to demonstrate split shipments. Carriers and tracking references are invented; no carrier API or live tracking is used.

| Field | Meaning |
|---|---|
| consignment_id | Unique consignment identifier for this snapshot |
| order_id | Link to the retail order |
| carrier | Sample carrier name |
| service_level | STANDARD or NEXT_DAY sample service |
| tracking_reference | Synthetic tracking reference |
| shipment_status | CREATED, DISPATCHED, IN_TRANSIT, DELIVERED, DELAYED, RETURNED or CANCELED |
| dispatched_at | Dispatch time (optional ISO text) |
| expected_delivery_date | Expected delivery date (optional ISO text) |
| delivered_at | Actual delivery time (optional ISO text) |

ADF stages shipments.json unchanged alongside order-items JSON and the converted orders.parquet. Databricks validates consignment IDs, required fields, status reference values and order links. It writes accepted rows to `dbo.shipment_reporting` and rejected rows to an ADLS Parquet folder. ADF waits for all three source copies before validation.

Every duplicate consignment ID is rejected. Unknown order IDs and orders that failed validation receive distinct rejection reasons. Reporting stays at **one row per consignment**, separately from sales totals, so split shipments cannot inflate order values. Orders without shipments remain valid. Dates are retained as source text; date chronology and SLA checks are not implemented.

For a new database run `sql/setup.sql` followed by `sql/002_shipments.sql`. For an existing demo database, run only `sql/002_shipments.sql` before the updated pipeline. The two reporting snapshots are refreshed sequentially and are not an atomic transaction; rerun the complete immutable batch after a partial failure.

## Included assets
- `data/`: invented grocery orders, order items, shipments, customers and order/shipment status references.
- `src/shipments.mjs`, `tests/shipments.test.mjs`: consignment validation and tests.
- `sql/002_shipments.sql`: shipment status reference and reporting migration.
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

## Verification
All 16 local tests passed: seven order tests, five shipment tests and four pipeline/schema contract checks. Azure/Databricks execution has not been verified in a live subscription. Cloud assets require linked services, permissions, secrets and resources described in the deployment guide. No production performance or ASDA operational results are claimed.
