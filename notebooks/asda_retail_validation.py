# Databricks notebook source
# ASDA sample-data portfolio replica. See README and docs/deployment.md.
# COMMAND ----------
import json
import re
from pyspark.sql import functions as F, Window

for name, default in {
    "landing_root": "", "output_root": "", "run_id": "",
    "secret_scope": "asda-demo", "sql_server": "", "sql_database": ""
}.items():
    dbutils.widgets.text(name, default)

cfg = {name: dbutils.widgets.get(name).strip() for name in
       ["landing_root", "output_root", "run_id", "secret_scope", "sql_server", "sql_database"]}
if not all(cfg.values()):
    raise ValueError("All notebook parameters are required")
if not re.fullmatch(r"[A-Za-z0-9_-]+", cfg["run_id"]):
    raise ValueError("run_id must be a safe path segment")
if not re.fullmatch(r"[A-Za-z0-9.-]+", cfg["sql_server"]) or not re.fullmatch(r"[A-Za-z0-9_-]+", cfg["sql_database"]):
    raise ValueError("Invalid SQL endpoint configuration")

landing = cfg["landing_root"].rstrip("/") + "/" + cfg["run_id"]
output = cfg["output_root"].rstrip("/") + "/" + cfg["run_id"]
jdbc_url = (
    f"jdbc:sqlserver://{cfg['sql_server']}:1433;databaseName={cfg['sql_database']};"
    "encrypt=true;trustServerCertificate=false;loginTimeout=30;"
)
jdbc_properties = {
    "user": dbutils.secrets.get(cfg["secret_scope"], "sql-user"),
    "password": dbutils.secrets.get(cfg["secret_scope"], "sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
# Storage access must be configured with an approved identity/external location.
# Do not embed account keys or print connection properties.

# COMMAND ----------
orders = (spark.read.schema(
    "order_id STRING, order_date STRING, customer_id STRING, order_status STRING, store_id STRING"
).parquet(landing + "/orders.parquet"))
for column in ["order_id", "order_date", "customer_id", "order_status"]:
    orders = orders.withColumn(column, F.trim(F.coalesce(F.col(column), F.lit(""))))
orders = orders.withColumn("order_status", F.upper("order_status")).cache()
items = (spark.read.schema(
    "item_id STRING, order_id STRING, product_name STRING, quantity LONG, unit_price_pence LONG"
).option("mode", "FAILFAST").json(landing + "/order_items.json"))
for column in ["item_id", "order_id"]:
    items = items.withColumn(column, F.trim(F.coalesce(F.col(column), F.lit(""))))
items = items.cache()
customers = spark.read.jdbc(jdbc_url, "dbo.customers", properties=jdbc_properties)
customers = customers.select(F.trim("customer_id").alias("customer_id")).cache()
statuses = (spark.read.jdbc(jdbc_url, "dbo.valid_order_status", properties=jdbc_properties)
    .select(F.upper(F.trim("status_name")).alias("order_status"))
    .filter(F.col("order_status").isNotNull() & (F.col("order_status") != "")).distinct())

if customers.filter(F.col("customer_id").isNull() | (F.col("customer_id") == "")).limit(1).count():
    raise ValueError("Invalid customer reference")
if customers.count() != customers.select("customer_id").distinct().count():
    raise ValueError("Duplicate customer reference")
if not statuses.limit(1).count():
    raise ValueError("Empty status reference")

# COMMAND ----------
# Fail malformed item batches before any reporting writes.
bad_item = ((F.col("item_id") == "") | (F.col("order_id") == "") |
    F.col("quantity").isNull() | (F.col("quantity") <= 0) |
    F.col("unit_price_pence").isNull() | (F.col("unit_price_pence") < 0))
if items.filter(bad_item).limit(1).count():
    raise ValueError("Invalid item values")
if items.groupBy("item_id").count().filter("count > 1").limit(1).count():
    raise ValueError("Duplicate item IDs")
if items.join(orders.select("order_id").distinct(), "order_id", "left_anti").limit(1).count():
    raise ValueError("Orphan items")

staged = orders.withColumn("_id_count", F.count("*").over(Window.partitionBy("order_id")))
staged = staged.join(statuses.withColumn("_known_status", F.lit(True)), "order_status", "left")
staged = staged.join(customers.withColumn("_known_customer", F.lit(True)), "customer_id", "left")
staged = (staged.withColumn("rejection_reason",
    F.when((F.col("order_id") == "") | (F.col("order_date") == "") | (F.col("customer_id") == ""),
           F.lit("MISSING_REQUIRED_FIELD"))
     .when(F.col("_id_count") > 1, F.lit("DUPLICATE_ORDER_ID"))
     .when(F.col("_known_status").isNull(), F.lit("INVALID_ORDER_STATUS"))
     .when(F.col("_known_customer").isNull(), F.lit("UNKNOWN_CUSTOMER")))
    .drop("_id_count", "_known_status", "_known_customer")
    .withColumn("pipeline_run_id", F.lit(cfg["run_id"]))
    .withColumn("source_file", F.lit(landing + "/orders.parquet"))
    .withColumn("processed_at", F.current_timestamp()).cache())
rejected = staged.filter(F.col("rejection_reason").isNotNull())
accepted = staged.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")

# Decimal arithmetic avoids long multiplication overflow before aggregation.
totals = items.groupBy("order_id").agg(F.sum(
    F.col("quantity").cast("decimal(18,0)") * F.col("unit_price_pence").cast("decimal(18,0)")
).alias("total_pence"))
if totals.filter(F.col("total_pence").isNull() | (F.col("total_pence") > F.lit(9007199254740991))).limit(1).count():
    raise ValueError("Money overflow")
if accepted.join(totals, "order_id", "left_anti").limit(1).count():
    raise ValueError("Accepted order missing items")
report = (accepted.join(totals, "order_id")
    .withColumn("total_pence", F.col("total_pence").cast("long"))
    .withColumn("currency", F.lit("GBP")).select(
        "order_id", "order_date", "customer_id", "order_status", "store_id",
        "total_pence", "currency", "pipeline_run_id", "source_file", "processed_at").cache())
input_count, accepted_count, rejected_count = orders.count(), report.count(), rejected.count()
if input_count != accepted_count + rejected_count:
    raise ValueError("Order reconciliation failed")

# COMMAND ----------
# Shipments are a consignment snapshot, not a tracking-event history.
shipments = (spark.read.schema(
    "consignment_id STRING, order_id STRING, carrier STRING, service_level STRING, "
    "tracking_reference STRING, shipment_status STRING, dispatched_at STRING, "
    "expected_delivery_date STRING, delivered_at STRING"
).option("mode", "FAILFAST").json(landing + "/shipments.json"))
for column in ["consignment_id", "order_id", "carrier", "shipment_status"]:
    shipments = shipments.withColumn(column, F.trim(F.coalesce(F.col(column), F.lit(""))))
shipments = shipments.withColumn("shipment_status", F.upper("shipment_status")).cache()
shipment_statuses = (spark.read.jdbc(jdbc_url, "dbo.valid_shipment_status", properties=jdbc_properties)
    .select(F.upper(F.trim("status_name")).alias("shipment_status"))
    .filter(F.col("shipment_status").isNotNull() & (F.col("shipment_status") != "")).distinct())
if not shipment_statuses.limit(1).count():
    raise ValueError("Empty shipment status reference")
known_orders = orders.select("order_id").filter(F.col("order_id") != "").distinct()
shipment_staged = (shipments
    .withColumn("_consignment_count", F.count("*").over(Window.partitionBy("consignment_id")))
    .join(shipment_statuses.withColumn("_known_shipment_status", F.lit(True)), "shipment_status", "left")
    .join(known_orders.withColumn("_known_order", F.lit(True)), "order_id", "left")
    .join(report.select("order_id").withColumn("_approved_order", F.lit(True)), "order_id", "left"))
shipment_staged = (shipment_staged.withColumn("rejection_reason",
    F.when((F.col("consignment_id") == "") | (F.col("order_id") == "") | (F.col("carrier") == ""),
           F.lit("MISSING_SHIPMENT_FIELD"))
     .when(F.col("_consignment_count") > 1, F.lit("DUPLICATE_CONSIGNMENT_ID"))
     .when(F.col("_known_shipment_status").isNull(), F.lit("INVALID_SHIPMENT_STATUS"))
     .when(F.col("_known_order").isNull(), F.lit("UNKNOWN_ORDER"))
     .when(F.col("_approved_order").isNull(), F.lit("ORDER_NOT_APPROVED")))
    .drop("_consignment_count", "_known_shipment_status", "_known_order", "_approved_order")
    .withColumn("pipeline_run_id", F.lit(cfg["run_id"]))
    .withColumn("source_file", F.lit(landing + "/shipments.json"))
    .withColumn("processed_at", F.current_timestamp()).cache())
rejected_shipments = shipment_staged.filter(F.col("rejection_reason").isNotNull())
shipment_report = (shipment_staged.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
    .select("consignment_id", "order_id", "carrier", "service_level", "tracking_reference",
            "shipment_status", "dispatched_at", "expected_delivery_date", "delivered_at",
            "pipeline_run_id", "source_file", "processed_at").cache())
shipment_input, shipment_accepted, shipment_rejected = shipments.count(), shipment_report.count(), rejected_shipments.count()
if shipment_input != shipment_accepted + shipment_rejected:
    raise ValueError("Shipment reconciliation failed")

# COMMAND ----------
# Persist investigation evidence first; preserve raw source files.
rejected.write.mode("overwrite").option("compression", "snappy").parquet(output + "/rejected_orders")
rejected_shipments.write.mode("overwrite").option("compression", "snappy").parquet(output + "/rejected_shipments")
# Dedicated DEMO snapshot only. This multi-partition JDBC refresh is not atomic.
(report.write.option("truncate", "true").jdbc(
    jdbc_url, "dbo.sales_reporting", mode="overwrite", properties=jdbc_properties))
(shipment_report.write.option("truncate", "true").jdbc(
    jdbc_url, "dbo.shipment_reporting", mode="overwrite", properties=jdbc_properties))
audit = {
    "pipeline_run_id": cfg["run_id"], "input_orders": input_count,
    "accepted_orders": accepted_count, "rejected_orders": rejected_count,
    "total_pence": int(report.agg(F.sum("total_pence")).first()[0] or 0),
    "reasons": {r["rejection_reason"]: r["count"] for r in rejected.groupBy("rejection_reason").count().collect()}
}
audit["shipments"] = {"input_shipments": shipment_input, "accepted_shipments": shipment_accepted,
    "rejected_shipments": shipment_rejected,
    "reasons": {r["rejection_reason"]: r["count"] for r in rejected_shipments.groupBy("rejection_reason").count().collect()}}
spark.createDataFrame([(json.dumps(audit),)], ["value"]).write.mode("overwrite").text(output + "/audit")
dbutils.notebook.exit(json.dumps(audit))
