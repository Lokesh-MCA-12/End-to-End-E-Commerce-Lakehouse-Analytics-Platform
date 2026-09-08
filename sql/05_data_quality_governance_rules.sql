-- =============================================================================
-- Microsoft Fabric E-Commerce Lakehouse Platform
-- Automated Data Quality & Governance Framework — Rule Definitions & DDLs
-- =============================================================================

-- 1. Create Audit Results Table
CREATE TABLE IF NOT EXISTS data_quality_results (
    table_name VARCHAR(100),
    check_name VARCHAR(150),
    category VARCHAR(50),      -- COMPLETENESS, UNIQUENESS, INTEGRITY, VALIDITY, ACCURACY
    severity VARCHAR(20),      -- CRITICAL, HIGH, MEDIUM, LOW
    total_records BIGINT,
    failed_records BIGINT,
    success_percentage DOUBLE,
    status VARCHAR(20),        -- PASSED, FAILED, WARNING
    execution_timestamp TIMESTAMP
);

-- 2. Create Enterprise Data Catalog & Metadata Repository
CREATE TABLE IF NOT EXISTS data_governance_catalog (
    table_name VARCHAR(100),
    layer VARCHAR(20),         -- Bronze, Silver, Gold
    column_name VARCHAR(100),
    data_type VARCHAR(50),
    pii_classification VARCHAR(50), -- PUBLIC_ID, PII_CONFIDENTIAL, PII_RESTRICTED, FINANCIAL_RESTRICTED
    quality_rules_applied VARCHAR(255),
    retention_days INT,
    owner VARCHAR(100)
);

-- 3. Core Governance Assertions (Sample Audit Queries)

-- Rule 01: COMPLETENESS — Customer Key Null Check
SELECT 'silver_customers' AS table_name, 'check_null_customer_id' AS check_name,
       COUNT(*) AS total_records,
       SUM(CASE WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 1 ELSE 0 END) AS failed_records
FROM silver_customers;

-- Rule 02: UNIQUENESS — Customer Primary Key Check
SELECT 'silver_customers' AS table_name, 'check_duplicate_customer_id' AS check_name,
       COUNT(*) AS total_records,
       (COUNT(*) - COUNT(DISTINCT customer_id)) AS failed_records
FROM silver_customers;

-- Rule 03: REFERENTIAL INTEGRITY — Orders to Customer FK Check
SELECT 'silver_orders' AS table_name, 'check_orphaned_customer_id' AS check_name,
       COUNT(*) AS total_records,
       SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS failed_records
FROM silver_orders o
LEFT JOIN silver_customers c ON o.customer_id = c.customer_id;

-- Rule 04: ACCURACY — Line Total Reconciliation
SELECT 'silver_sales' AS table_name, 'check_line_total_reconciliation' AS check_name,
       COUNT(*) AS total_records,
       SUM(CASE WHEN ABS(sales_amount - ((quantity * unit_price) - discount)) > 0.01 THEN 1 ELSE 0 END) AS failed_records
FROM silver_sales;

-- Rule 05: REFERENTIAL INTEGRITY — Gold Star Schema Dimension Key Checks
SELECT 'fact_sales' AS table_name, 'check_orphaned_customer_key' AS check_name,
       COUNT(*) AS total_records,
       SUM(CASE WHEN c.customer_key IS NULL THEN 1 ELSE 0 END) AS failed_records
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_key = c.customer_key;
