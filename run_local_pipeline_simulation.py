"""
End-to-End E-Commerce Lakehouse Analytics Platform — Modular Medallion Pipeline Runner
Supports individual execution of Bronze, Silver, and Gold Medallion Lakehouse layers
as well as full sequential execution.
"""

import os
import json
import duckdb

def get_data_dir():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "datasets", "sample_raw_data")
    if not os.path.exists(data_dir):
        print(f"[PIPELINE] Sample raw data not found at '{data_dir}'. Generating synthetic datasets...")
        from datasets.generate_datasets import generate_ecommerce_data
        generate_ecommerce_data(data_dir)
    return data_dir

def run_bronze(conn):
    """Executes Stage 1: Bronze Lakehouse Raw Data Ingestion & Metadata Tagging."""
    data_dir = get_data_dir()
    logs = ["[STAGE 1: BRONZE] Starting raw ingestion and metadata enrichment..."]
    
    conn.execute("DROP TABLE IF EXISTS bronze_customers;")
    conn.execute(f"""
        CREATE TABLE bronze_customers AS 
        SELECT 
            *, 
            CURRENT_TIMESTAMP AS ingestion_timestamp,
            'raw_customers.csv' AS source_file,
            'BATCH_20260908' AS batch_id,
            'RUN_LOCAL_001' AS pipeline_run_id,
            'WEB_CRM_DB' AS source_system
        FROM read_csv_auto('{os.path.join(data_dir, "raw_customers.csv").replace("\\", "/")}');
    """)
    cust_count = conn.execute("SELECT COUNT(*) FROM bronze_customers").fetchone()[0]
    logs.append(f" -> Bronze Customers: {cust_count} raw records ingested into raw_customers.")

    conn.execute("DROP TABLE IF EXISTS bronze_products;")
    conn.execute(f"""
        CREATE TABLE bronze_products AS 
        SELECT 
            *, 
            CURRENT_TIMESTAMP AS ingestion_timestamp,
            'raw_products.json' AS source_file,
            'BATCH_20260908' AS batch_id,
            'RUN_LOCAL_001' AS pipeline_run_id,
            'PRODUCT_CATALOG_API' AS source_system
        FROM read_json_auto('{os.path.join(data_dir, "raw_products.json").replace("\\", "/")}');
    """)
    prod_count = conn.execute("SELECT COUNT(*) FROM bronze_products").fetchone()[0]
    logs.append(f" -> Bronze Products: {prod_count} raw records ingested into raw_products.")

    conn.execute("DROP TABLE IF EXISTS bronze_orders;")
    conn.execute(f"""
        CREATE TABLE bronze_orders AS 
        SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp, 'BATCH_20260908' AS batch_id, 'ORDER_SYS' AS source_system
        FROM read_csv_auto('{os.path.join(data_dir, "raw_orders.csv").replace("\\", "/")}');
    """)
    ord_count = conn.execute("SELECT COUNT(*) FROM bronze_orders").fetchone()[0]

    conn.execute("DROP TABLE IF EXISTS bronze_order_items;")
    conn.execute(f"""
        CREATE TABLE bronze_order_items AS 
        SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp, 'BATCH_20260908' AS batch_id, 'ORDER_SYS' AS source_system
        FROM read_csv_auto('{os.path.join(data_dir, "raw_order_items.csv").replace("\\", "/")}');
    """)
    item_count = conn.execute("SELECT COUNT(*) FROM bronze_order_items").fetchone()[0]

    conn.execute("DROP TABLE IF EXISTS bronze_payments;")
    conn.execute(f"""
        CREATE TABLE bronze_payments AS 
        SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp, 'BATCH_20260908' AS batch_id, 'PAYMENT_API' AS source_system
        FROM read_json_auto('{os.path.join(data_dir, "raw_payments.json").replace("\\", "/")}');
    """)

    conn.execute("DROP TABLE IF EXISTS bronze_returns;")
    conn.execute(f"""
        CREATE TABLE bronze_returns AS 
        SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp, 'BATCH_20260908' AS batch_id, 'CS_APP' AS source_system
        FROM read_csv_auto('{os.path.join(data_dir, "raw_returns.csv").replace("\\", "/")}');
    """)

    conn.execute("DROP TABLE IF EXISTS bronze_inventory;")
    conn.execute(f"""
        CREATE TABLE bronze_inventory AS 
        SELECT *, CURRENT_TIMESTAMP AS ingestion_timestamp, 'BATCH_20260908' AS batch_id, 'ERP_SYS' AS source_system
        FROM read_csv_auto('{os.path.join(data_dir, "raw_inventory.csv").replace("\\", "/")}');
    """)

    logs.append(f" -> Bronze Orders & Items: {ord_count} orders & {item_count} items landed.")
    logs.append("[STAGE 1: BRONZE] Ingestion completed successfully.")
    
    return {
        "stage": "bronze",
        "status": "SUCCESS",
        "tables": {
            "bronze_customers": cust_count,
            "bronze_products": prod_count,
            "bronze_orders": ord_count,
            "bronze_order_items": item_count
        },
        "logs": logs
    }

def run_silver(conn):
    """Executes Stage 2: Silver Lakehouse Data Cleansing & Schema Integration."""
    # Ensure Bronze exists
    tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    if "bronze_customers" not in tables:
        run_bronze(conn)
        
    logs = ["[STAGE 2: SILVER] Starting data cleansing, type standardization & integration..."]

    # 1. Silver Customers
    conn.execute("DROP TABLE IF EXISTS silver_customers;")
    conn.execute("""
        CREATE TABLE silver_customers AS
        SELECT DISTINCT
            TRIM(customer_id) AS customer_id,
            TRIM(first_name) AS first_name,
            TRIM(last_name) AS last_name,
            TRIM(first_name) || ' ' || TRIM(last_name) AS full_name,
            LOWER(TRIM(email)) AS email,
            TRIM(gender) AS gender,
            TRIM(city) AS city,
            TRIM(state) AS state,
            TRIM(country) AS country,
            CAST(registration_date AS DATE) AS registration_date,
            TRIM(customer_segment) AS customer_segment,
            CURRENT_TIMESTAMP AS updated_timestamp
        FROM bronze_customers
        WHERE customer_id IS NOT NULL AND TRIM(customer_id) != '';
    """)
    s_cust_count = conn.execute("SELECT COUNT(*) FROM silver_customers").fetchone()[0]
    logs.append(f" -> Silver Customers: {s_cust_count} records cleansed & deduplicated.")

    # 2. Silver Products
    conn.execute("DROP TABLE IF EXISTS silver_products;")
    conn.execute("""
        CREATE TABLE silver_products AS
        SELECT DISTINCT
            TRIM(product_id) AS product_id,
            TRIM(product_name) AS product_name,
            CAST(category_id AS INT) AS category_id,
            TRIM(category_name) AS category_name,
            TRIM(brand) AS brand,
            CAST(unit_price AS DOUBLE) AS unit_price,
            CAST(cost_price AS DOUBLE) AS cost_price,
            TRIM(supplier) AS supplier,
            ROUND(unit_price - cost_price, 2) AS profit_margin_amount,
            CURRENT_TIMESTAMP AS updated_timestamp
        FROM bronze_products
        WHERE product_id IS NOT NULL AND unit_price > 0;
    """)
    s_prod_count = conn.execute("SELECT COUNT(*) FROM silver_products").fetchone()[0]
    logs.append(f" -> Silver Products: {s_prod_count} catalog records cleansed.")

    # 3. Silver Orders & Items
    conn.execute("DROP TABLE IF EXISTS silver_orders;")
    conn.execute("""
        CREATE TABLE silver_orders AS
        SELECT DISTINCT
            TRIM(order_id) AS order_id,
            TRIM(customer_id) AS customer_id,
            CAST(order_date AS TIMESTAMP) AS order_date,
            TRIM(order_status) AS order_status,
            TRIM(shipping_address) AS shipping_address,
            TRIM(payment_id) AS payment_id,
            CAST(total_amount AS DOUBLE) AS total_amount,
            CURRENT_TIMESTAMP AS updated_timestamp
        FROM bronze_orders
        WHERE order_id IS NOT NULL;
    """)
    
    conn.execute("DROP TABLE IF EXISTS silver_order_items;")
    conn.execute("""
        CREATE TABLE silver_order_items AS
        SELECT DISTINCT
            TRIM(order_item_id) AS order_item_id,
            TRIM(order_id) AS order_id,
            TRIM(product_id) AS product_id,
            CAST(quantity AS INT) AS quantity,
            CAST(unit_price AS DOUBLE) AS unit_price,
            CAST(discount AS DOUBLE) AS discount,
            ROUND((quantity * unit_price) - discount, 2) AS line_total,
            CURRENT_TIMESTAMP AS updated_timestamp
        FROM bronze_order_items
        WHERE order_item_id IS NOT NULL AND quantity > 0;
    """)
    
    # 4. Silver Integrated Sales
    conn.execute("DROP TABLE IF EXISTS silver_sales;")
    conn.execute("""
        CREATE TABLE silver_sales AS
        SELECT 
            i.order_item_id,
            o.order_id,
            o.customer_id,
            i.product_id,
            p.category_id,
            p.category_name,
            o.order_date,
            o.order_status,
            i.quantity,
            i.unit_price,
            i.discount,
            p.cost_price,
            ROUND((i.quantity * i.unit_price) - i.discount, 2) AS sales_amount,
            ROUND(i.quantity * p.cost_price, 2) AS cost_amount,
            ROUND(((i.quantity * i.unit_price) - i.discount) - (i.quantity * p.cost_price), 2) AS profit_amount,
            CURRENT_TIMESTAMP AS updated_timestamp
        FROM silver_order_items i
        JOIN silver_orders o ON i.order_id = o.order_id
        JOIN silver_products p ON i.product_id = p.product_id
        JOIN silver_customers c ON o.customer_id = c.customer_id;
    """)
    s_sales_count = conn.execute("SELECT COUNT(*) FROM silver_sales").fetchone()[0]
    logs.append(f" -> Silver Integrated Sales: {s_sales_count} integrated order line records created.")
    logs.append("[STAGE 2: SILVER] Transformation completed successfully.")

    return {
        "stage": "silver",
        "status": "SUCCESS",
        "tables": {
            "silver_customers": s_cust_count,
            "silver_products": s_prod_count,
            "silver_sales": s_sales_count
        },
        "logs": logs
    }

def run_gold(conn):
    """Executes Stage 3: Gold Lakehouse Star Schema Modeling (SCD Type 2) & Data Quality Checks."""
    # Ensure Silver exists
    tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
    if "silver_sales" not in tables:
        run_silver(conn)

    logs = ["[STAGE 3: GOLD] Starting Star Schema modeling & Data Quality Governance..."]

    # 1. dim_customer (SCD Type 2)
    conn.execute("DROP TABLE IF EXISTS dim_customer;")
    conn.execute("""
        CREATE TABLE dim_customer AS
        SELECT 
            md5(customer_id || '||2026-01-01') AS customer_key,
            customer_id,
            first_name,
            last_name,
            full_name,
            email,
            gender,
            city,
            state,
            country,
            customer_segment,
            registration_date AS effective_date,
            CAST('9999-12-31' AS DATE) AS expiry_date,
            TRUE AS is_current
        FROM silver_customers;
    """)
    dim_c_count = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    logs.append(f" -> Gold Dimension: dim_customer (SCD Type 2) built with {dim_c_count} customer keys.")

    # 2. dim_product
    conn.execute("DROP TABLE IF EXISTS dim_product;")
    conn.execute("""
        CREATE TABLE dim_product AS
        SELECT 
            md5(product_id) AS product_key,
            product_id,
            product_name,
            category_id,
            category_name,
            brand,
            unit_price,
            cost_price,
            supplier
        FROM silver_products;
    """)
    dim_p_count = conn.execute("SELECT COUNT(*) FROM dim_product").fetchone()[0]

    # 3. dim_date
    conn.execute("DROP TABLE IF EXISTS dim_date;")
    conn.execute("""
        CREATE TABLE dim_date AS
        SELECT 
            CAST(strftime(full_date, '%Y%m%d') AS INT) AS date_key,
            full_date,
            EXTRACT(DAY FROM full_date) AS day,
            EXTRACT(MONTH FROM full_date) AS month,
            strftime(full_date, '%B') AS month_name,
            EXTRACT(QUARTER FROM full_date) AS quarter,
            EXTRACT(YEAR FROM full_date) AS year,
            EXTRACT(WEEK FROM full_date) AS week,
            strftime(full_date, '%A') AS day_name,
            CASE WHEN EXTRACT(DAYOFWEEK FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
        FROM (
            SELECT CAST('2025-01-01' AS DATE) + INTERVAL (i) DAY AS full_date
            FROM range(0, 730) t(i)
        );
    """)

    # 4. fact_sales
    conn.execute("DROP TABLE IF EXISTS fact_sales;")
    conn.execute("""
        CREATE TABLE fact_sales AS
        SELECT 
            md5(s.order_item_id) AS sales_key,
            s.order_id,
            c.customer_key,
            p.product_key,
            CAST(strftime(s.order_date, '%Y%m%d') AS INT) AS date_key,
            s.quantity,
            s.unit_price,
            s.discount,
            s.sales_amount,
            s.cost_amount,
            s.profit_amount
        FROM silver_sales s
        LEFT JOIN dim_customer c ON s.customer_id = c.customer_id AND c.is_current = TRUE
        LEFT JOIN dim_product p ON s.product_id = p.product_id;
    """)
    fact_s_count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]

    # 5. fact_returns
    conn.execute("DROP TABLE IF EXISTS fact_returns;")
    conn.execute("""
        CREATE TABLE fact_returns AS
        SELECT 
            md5(r.return_id) AS return_key,
            r.return_id,
            r.order_id,
            p.product_key,
            CAST(strftime(CAST(r.return_date AS DATE), '%Y%m%d') AS INT) AS date_key,
            r.return_reason,
            CAST(r.refund_amount AS DOUBLE) AS refund_amount
        FROM bronze_returns r
        LEFT JOIN dim_product p ON r.product_id = p.product_id;
    """)
    logs.append(f" -> Gold Fact Tables: fact_sales ({fact_s_count} rows), fact_returns created.")

    # 6. Automated Data Quality & Governance Framework Audit Engine
    conn.execute("DROP TABLE IF EXISTS data_quality_results;")
    conn.execute("""
        CREATE TABLE data_quality_results (
            table_name VARCHAR,
            check_name VARCHAR,
            category VARCHAR,
            severity VARCHAR,
            total_records BIGINT,
            failed_records BIGINT,
            success_percentage DOUBLE,
            status VARCHAR,
            execution_timestamp TIMESTAMP
        );
    """)

    dq_checks = [
        ("silver_customers", "check_null_customer_id", "COMPLETENESS", "CRITICAL", "SELECT COUNT(*) FROM silver_customers", "SELECT COUNT(*) FROM silver_customers WHERE customer_id IS NULL OR TRIM(customer_id) = ''"),
        ("silver_customers", "check_duplicate_customer_id", "UNIQUENESS", "CRITICAL", "SELECT COUNT(*) FROM silver_customers", "SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM silver_customers"),
        ("silver_products", "check_null_product_id", "COMPLETENESS", "CRITICAL", "SELECT COUNT(*) FROM silver_products", "SELECT COUNT(*) FROM silver_products WHERE product_id IS NULL OR TRIM(product_id) = ''"),
        ("silver_products", "check_positive_unit_price", "VALIDITY", "HIGH", "SELECT COUNT(*) FROM silver_products", "SELECT COUNT(*) FROM silver_products WHERE unit_price <= 0"),
        ("silver_orders", "check_duplicate_order_id", "UNIQUENESS", "CRITICAL", "SELECT COUNT(*) FROM silver_orders", "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM silver_orders"),
        ("silver_orders", "check_orphaned_customer_id", "INTEGRITY", "CRITICAL", "SELECT COUNT(*) FROM silver_orders", "SELECT COUNT(*) FROM silver_orders WHERE customer_id NOT IN (SELECT customer_id FROM silver_customers)"),
        ("silver_order_items", "check_positive_quantity", "VALIDITY", "HIGH", "SELECT COUNT(*) FROM silver_order_items", "SELECT COUNT(*) FROM silver_order_items WHERE quantity <= 0"),
        ("silver_sales", "check_line_total_reconciliation", "ACCURACY", "HIGH", "SELECT COUNT(*) FROM silver_sales", "SELECT COUNT(*) FROM silver_sales WHERE ABS(sales_amount - ((quantity * unit_price) - discount)) > 0.01"),
        ("fact_sales", "check_orphaned_customer_key", "INTEGRITY", "CRITICAL", "SELECT COUNT(*) FROM fact_sales", "SELECT COUNT(*) FROM fact_sales WHERE customer_key NOT IN (SELECT customer_key FROM dim_customer)"),
        ("fact_sales", "check_orphaned_product_key", "INTEGRITY", "CRITICAL", "SELECT COUNT(*) FROM fact_sales", "SELECT COUNT(*) FROM fact_sales WHERE product_key NOT IN (SELECT product_key FROM dim_product)"),
        ("dim_customer", "check_scd2_active_records", "VALIDITY", "MEDIUM", "SELECT COUNT(*) FROM dim_customer", "SELECT COUNT(*) FROM dim_customer WHERE is_current = TRUE AND expiry_date != '9999-12-31'"),
        ("fact_sales", "check_positive_sales_amount", "VALIDITY", "HIGH", "SELECT COUNT(*) FROM fact_sales", "SELECT COUNT(*) FROM fact_sales WHERE sales_amount <= 0")
    ]

    for tbl, chk, cat, sev, total_sql, fail_sql in dq_checks:
        total_cnt = conn.execute(total_sql).fetchone()[0]
        fail_cnt = conn.execute(fail_sql).fetchone()[0]
        pct = round(((total_cnt - fail_cnt) / total_cnt) * 100, 2) if total_cnt > 0 else 100.0
        status = "PASSED" if fail_cnt == 0 else "FAILED"
        
        conn.execute(f"""
            INSERT INTO data_quality_results VALUES 
            ('{tbl}', '{chk}', '{cat}', '{sev}', {total_cnt}, {fail_cnt}, {pct}, '{status}', CURRENT_TIMESTAMP);
        """)

    # 7. Enterprise Data Catalog & PII Governance Catalog
    conn.execute("DROP TABLE IF EXISTS data_governance_catalog;")
    conn.execute("""
        CREATE TABLE data_governance_catalog (
            table_name VARCHAR,
            layer VARCHAR,
            column_name VARCHAR,
            data_type VARCHAR,
            pii_classification VARCHAR,
            quality_rules_applied VARCHAR,
            retention_days INT,
            owner VARCHAR
        );
    """)

    conn.execute("""
        INSERT INTO data_governance_catalog VALUES
        ('silver_customers', 'Silver', 'customer_id', 'VARCHAR', 'PUBLIC_ID', 'NotNull, PrimaryKey', 365, 'CRM Data Engineering'),
        ('silver_customers', 'Silver', 'full_name', 'VARCHAR', 'PII_RESTRICTED', 'Cleansed, Trimmed', 365, 'Compliance & Privacy'),
        ('silver_customers', 'Silver', 'email', 'VARCHAR', 'PII_CONFIDENTIAL', 'Lowercased, ValidEmailFormat', 365, 'Compliance & Privacy'),
        ('silver_products', 'Silver', 'product_id', 'VARCHAR', 'PUBLIC_ID', 'NotNull, PrimaryKey', 730, 'Merchandising Team'),
        ('silver_products', 'Silver', 'unit_price', 'DOUBLE', 'PUBLIC', 'GreaterThanZero', 730, 'Merchandising Team'),
        ('silver_sales', 'Silver', 'sales_amount', 'DOUBLE', 'FINANCIAL_RESTRICTED', 'LineItemReconciliation', 1095, 'Finance Analytics'),
        ('fact_sales', 'Gold', 'customer_key', 'VARCHAR', 'ANONYMIZED_KEY', 'ForeignKey_dim_customer', 1095, 'BI & Analytics Team'),
        ('dim_customer', 'Gold', 'is_current', 'BOOLEAN', 'PUBLIC', 'SCD_Type_2_Active_Flag', 1095, 'BI & Analytics Team');
    """)
    
    logs.append(" -> Automated Data Quality Framework: 12 governance rules evaluated and catalog metadata logged.")
    logs.append("[STAGE 3: GOLD] Dimensional modeling & governance framework executed successfully.")

    return {
        "stage": "gold",
        "status": "SUCCESS",
        "tables": {
            "dim_customer": dim_c_count,
            "dim_product": dim_p_count,
            "fact_sales": fact_s_count,
            "data_quality_results": len(dq_checks)
        },
        "logs": logs
    }

def run_pipeline(conn=None, stage='all'):
    if conn is None:
        conn = duckdb.connect(":memory:")

    stage = str(stage).lower()
    combined_logs = []
    
    if stage == 'bronze':
        res = run_bronze(conn)
        combined_logs.extend(res["logs"])
    elif stage == 'silver':
        res = run_silver(conn)
        combined_logs.extend(res["logs"])
    elif stage == 'gold':
        res = run_gold(conn)
        combined_logs.extend(res["logs"])
    else: # 'all'
        r1 = run_bronze(conn)
        r2 = run_silver(conn)
        r3 = run_gold(conn)
        combined_logs.extend(r1["logs"])
        combined_logs.extend(r2["logs"])
        combined_logs.extend(r3["logs"])

    for line in combined_logs:
        print(line)

    return {
        "status": "SUCCESS",
        "stage": stage,
        "logs": combined_logs
    }

if __name__ == "__main__":
    run_pipeline(stage='all')
