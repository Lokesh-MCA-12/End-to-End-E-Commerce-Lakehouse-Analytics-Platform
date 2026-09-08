# Technical Implementation & Deployment Guide

## Prerequisites & Setup
1. **Microsoft Fabric Capacity**: Active Fabric trial or F-SKU capacity enabled.
2. **Fabric Workspace**: Create a dedicated workspace (e.g., `ECommerce-Lakehouse-Analytics`).
3. **Storage Substrate**: Provision three Lakehouses inside the workspace:
   - `Bronze_Lakehouse`
   - `Silver_Lakehouse`
   - `Gold_Lakehouse`
4. **Git Repository Binding**: Connect workspace to GitHub repository `ecommerce-fabric-lakehouse`.

## Deployment Sequence

### Step 1: Data Ingestion (Bronze Layer)
Deploy pipeline `PL_Ecommerce_Ingestion` and upload raw CSV/JSON files into `Bronze_Lakehouse/Files`. Run ingestion notebooks:
- `01_ingest_customers`
- `02_ingest_products`
- `03_ingest_orders`

### Step 2: Cleansing & Integration (Silver Layer)
Run Silver transformation PySpark notebooks:
- `04_transform_customers`
- `05_transform_products`
- `06_transform_orders`
- `07_transform_sales`

### Step 3: Dimensional Modeling (Gold Layer)
Execute Gold Star Schema modeling notebooks:
- `08_build_dimensions` (SCD Type 2 processing)
- `09_build_fact_sales`
- `10_data_quality` (Logs execution results to `data_quality_results`)
- `11_incremental_processing`
- `12_gold_aggregations`

### Step 4: SQL Endpoint & Power BI Direct Lake Setup
1. Switch to the **Fabric SQL Analytics Endpoint** view over `Gold_Lakehouse`.
2. Verify T-SQL queries under `sql/analytics/`.
3. Create new **Semantic Model** choosing `fact_sales`, `fact_returns`, `dim_customer`, `dim_product`, `dim_date`.
4. Set mode to **Direct Lake**.
5. Import DAX measures from `powerbi/dax_measures.dax`.
