# Microsoft Fabric Production Lakehouse Deployment Guide

This guide details how the **End-to-End E-Commerce Lakehouse Analytics Platform** deploys natively inside **Microsoft Fabric** using **OneLake**, **Delta Lake**, **PySpark Notebooks**, **Fabric Data Factory Pipelines**, **Serverless T-SQL Analytics Endpoint**, and **Power BI Direct Lake Mode**.

---

## 1. Microsoft Fabric Workspace & OneLake Architecture

In Microsoft Fabric, the platform uses three dedicated Lakehouses residing inside a unified SaaS OneLake substrate:

```
+---------------------------------------------------------------------------------------------------+
|                                  MICROSOFT FABRIC WORKSPACE                                       |
|                               (e.g., ECommerce-Lakehouse-Analytics)                               |
|                                                                                                   |
|  [ Bronze_Lakehouse ]                                                                             |
|   ├── Files/                          <-- Raw landing zone (CSV, JSON files from pipelines)       |
|   └── Tables/                         <-- Delta tables (raw_customers, raw_products, raw_orders)  |
|                                                                                                   |
|  [ Silver_Lakehouse ]                                                                             |
|   └── Tables/                         <-- Delta tables (silver_customers, silver_products, etc.) |
|                                                                                                   |
|  [ Gold_Lakehouse ]                                                                               |
|   └── Tables/                         <-- Star Schema Delta tables (fact_sales, dim_customer SCD2)|
+---------------------------------------------------------------------------------------------------+
```

### OneLake ABFSS Path Specifications
| Lakehouse | OneLake ABFSS Storage Path |
| :--- | :--- |
| **Bronze Landing Files** | `abfss://ECommerce-Lakehouse@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/Files/` |
| **Bronze Delta Tables**  | `abfss://ECommerce-Lakehouse@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/Tables/` |
| **Silver Delta Tables**  | `abfss://ECommerce-Lakehouse@onelake.dfs.fabric.microsoft.com/Silver_Lakehouse.Lakehouse/Tables/` |
| **Gold Delta Tables**    | `abfss://ECommerce-Lakehouse@onelake.dfs.fabric.microsoft.com/Gold_Lakehouse.Lakehouse/Tables/`   |

---

## 2. Fabric Notebook Suite & Execution Model

The platform consists of **12 PySpark Notebooks** (`.ipynb` format) deployed to the workspace:

### Bronze Layer Notebooks
1. **`01_ingest_customers.ipynb`**: Reads raw customer CSVs from `Bronze_Lakehouse/Files/`, enriches records with operational lineage (`ingestion_timestamp`, `source_file`, `batch_id`, `pipeline_run_id`, `source_system`), and writes to `Bronze_Lakehouse.raw_customers` Delta table.
2. **`02_ingest_products.ipynb`**: Ingests JSON catalog files to `Bronze_Lakehouse.raw_products`.
3. **`03_ingest_orders.ipynb`**: Ingests Orders, Order Items, Payments, Returns, and Inventory datasets.

### Silver Layer Notebooks
4. **`04_transform_customers.ipynb`**: Cleanses raw customers, standardizes types, removes nulls, deduplicates, and writes to `Silver_Lakehouse.silver_customers` Delta table.
5. **`05_transform_products.ipynb`**: Cleanses catalog items, calculates unit margins, writes to `Silver_Lakehouse.silver_products`.
6. **`06_transform_orders.ipynb`**: Validates order statuses, total amounts, writes to `Silver_Lakehouse.silver_orders` & `silver_order_items`.
7. **`07_transform_sales.py`**: Joins Silver orders, items, products, customers, and payments to produce `Silver_Lakehouse.silver_sales`.

### Gold Layer Notebooks
8. **`08_build_dimensions.ipynb`**: Constructs `Gold_Lakehouse.dim_customer` using **SCD Type 2** tracking (`customer_key`, `effective_date`, `expiry_date`, `is_current`), `dim_product`, and `dim_date`.
9. **`09_build_fact_sales.ipynb`**: Models Star Schema fact tables `Gold_Lakehouse.fact_sales` and `Gold_Lakehouse.fact_returns`.
10. **`10_data_quality.ipynb`**: Runs automated quality validations and appends audit metrics to `Gold_Lakehouse.data_quality_results`.
11. **`11_incremental_processing.ipynb`**: Executes Change Data Capture (CDC) with watermarking and Delta `MERGE` upsert operations.
12. **`12_gold_aggregations.ipynb`**: Computes pre-aggregated monthly financial summaries.

---

## 3. Fabric Data Factory Orchestration Pipeline (`PL_Ecommerce_Ingestion`)

The Data Factory pipeline orchestrates execution across all three medallion layers:

```
[Start Pipeline]
       │
       ▼
 ┌──────────────────────────────────────────────────────────┐
 │ BRONZE ACTIVITY STAGE                                    │
 │ Ingest_Bronze_Customers  Ingest_Bronze_Products  Orders  │
 └────────────────────────────┬─────────────────────────────┘
                              │ (On Success)
                              ▼
 ┌──────────────────────────────────────────────────────────┐
 │ SILVER ACTIVITY STAGE                                    │
 │ Transform_Customers      Transform_Products      Orders │
 │                            │                             │
 │                            ▼                             │
 │                   Transform_Silver_Sales                 │
 └────────────────────────────┬─────────────────────────────┘
                              │ (On Success)
                              ▼
 ┌──────────────────────────────────────────────────────────┐
 │ GOLD ACTIVITY STAGE                                      │
 │ Build_Gold_Dimensions  ──>  Build_Gold_Fact_Sales        │
 │                                    │                     │
 │                                    ▼                     │
 │                      Execute_Data_Quality_Framework       │
 └──────────────────────────────────────────────────────────┘
```

---

## 4. Serving Layer: SQL Analytics Endpoint & Power BI Direct Lake

### Fabric SQL Analytics Endpoint
Every Microsoft Fabric Lakehouse automatically generates a serverless **SQL Analytics Endpoint**. You can connect using T-SQL (via SSMS, Azure Data Studio, or Fabric Portal) to query Gold Delta tables:

```sql
-- Query Gold Fact & Dimensions over serverless T-SQL
SELECT 
    d.year,
    d.month_name,
    p.category_name,
    SUM(f.sales_amount) AS total_revenue,
    SUM(f.profit_amount) AS total_profit
FROM Gold_Lakehouse.dbo.fact_sales f
JOIN Gold_Lakehouse.dbo.dim_date d ON f.date_key = d.date_key
JOIN Gold_Lakehouse.dbo.dim_product p ON f.product_key = p.product_key
GROUP BY d.year, d.month_name, p.category_name
ORDER BY d.year, total_revenue DESC;
```

### Power BI Direct Lake Mode Setup
1. In Microsoft Fabric Workspace, select **`Gold_Lakehouse`**.
2. Click **New Semantic Model**.
3. Select `fact_sales`, `fact_returns`, `dim_customer`, `dim_product`, `dim_date`.
4. Ensure storage mode is set to **Direct Lake**.
5. Add explicit DAX measures (`Total Revenue`, `Total Orders`, `Average Order Value`, `Total Profit`).
6. Build Power BI dashboards with zero refresh latency directly over OneLake Parquet files!
