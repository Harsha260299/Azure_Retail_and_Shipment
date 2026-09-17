-- Apply to an existing demo database, or after sql/setup.sql for a new database.
IF OBJECT_ID('dbo.valid_shipment_status', 'U') IS NULL
BEGIN
 CREATE TABLE dbo.valid_shipment_status (status_name varchar(40) NOT NULL PRIMARY KEY);
 INSERT dbo.valid_shipment_status VALUES
 ('CREATED'),('DISPATCHED'),('IN_TRANSIT'),('DELIVERED'),('DELAYED'),('RETURNED'),('CANCELED');
END;
IF OBJECT_ID('dbo.shipment_reporting', 'U') IS NULL
BEGIN
 CREATE TABLE dbo.shipment_reporting (
  consignment_id varchar(80) NOT NULL PRIMARY KEY,
  order_id varchar(40) NOT NULL,
  carrier varchar(100) NOT NULL,
  service_level varchar(40) NULL,
  tracking_reference varchar(100) NULL,
  shipment_status varchar(40) NOT NULL,
  dispatched_at varchar(40) NULL,
  expected_delivery_date varchar(40) NULL,
  delivered_at varchar(40) NULL,
  pipeline_run_id varchar(100) NOT NULL,
  source_file varchar(1000) NOT NULL,
  processed_at datetime2 NOT NULL
 );
END;
-- Dates are source text in this demonstration; semantic date validation is not implemented.
-- No physical FK: both demo snapshots are independently truncated/refreshed.
-- The notebook enforces the accepted-order relationship before writing.
SELECT consignment_id, order_id, carrier, shipment_status, tracking_reference
FROM dbo.shipment_reporting ORDER BY consignment_id;
