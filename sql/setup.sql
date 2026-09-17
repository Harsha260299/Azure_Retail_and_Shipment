-- Run in a dedicated, empty demo Azure SQL database.
-- Synthetic identifiers only; no customer names, contact data or passwords.
CREATE TABLE dbo.valid_order_status (
    status_name varchar(40) NOT NULL PRIMARY KEY
);
INSERT dbo.valid_order_status VALUES
('PENDING'), ('PROCESSING'), ('COMPLETE'), ('CANCELED'), ('ON_HOLD');

CREATE TABLE dbo.customers (
    customer_id varchar(20) NOT NULL PRIMARY KEY,
    customer_label varchar(80) NOT NULL,
    region varchar(80) NOT NULL
);
INSERT dbo.customers VALUES
('C001', 'Sample customer 001', 'Leeds'),
('C002', 'Sample customer 002', 'Manchester'),
('C003', 'Sample customer 003', 'Bristol');

CREATE TABLE dbo.sales_reporting (
    order_id varchar(40) NOT NULL PRIMARY KEY,
    order_date varchar(40) NOT NULL,
    customer_id varchar(20) NOT NULL,
    order_status varchar(40) NOT NULL,
    store_id varchar(40) NULL,
    total_pence bigint NOT NULL,
    currency varchar(3) NOT NULL,
    pipeline_run_id varchar(100) NOT NULL,
    source_file varchar(1000) NOT NULL,
    processed_at datetime2 NOT NULL
);
GO
-- After the sample cloud run: 4 orders, 1750 pence.
SELECT COUNT(*) AS accepted_orders, SUM(total_pence) AS basket_value_pence
FROM dbo.sales_reporting;
