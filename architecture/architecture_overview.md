# Platform Architecture Overview

## Microsoft Fabric Medallion Lakehouse

The architecture leverages a **Medallion Lakehouse pattern** hosted inside a dedicated **Microsoft Fabric Workspace**:

```
+---------------------------------------------------------------------------------------+
|                                MICROSOFT FABRIC WORKSPACE                             |
|                                                                                       |
|  Fabric Data Factory Pipeline: PL_Ecommerce_Ingestion                                 |
|                                                                                       |
|  [BRONZE LAKEHOUSE]                                                                   |
|   - Ingests CSV/JSON from source APIs, CRM, ERP, and Web logs.                        |
|   - Appends lineage fields: ingestion_timestamp, source_file, batch_id, run_id.      |
|                                                                                       |
|  [SILVER LAKEHOUSE]                                                                   |
|   - Cleanse, type-cast, handle nulls, deduplicate, and enforce domain constraints.   |
|   - Store cleaned Delta Lake tables: silver_customers, silver_products, etc.          |
|                                                                                       |
|  [GOLD LAKEHOUSE]                                                                     |
|   - Business-ready Star Schema (Facts & Dimensions).                                  |
|   - dim_customer (SCD Type 2), dim_product, dim_date, fact_sales, fact_returns.      |
|                                                                                       |
|  [FABRIC SQL ANALYTICS ENDPOINT]                                                       |
|   - Serverless T-SQL querying over Gold Delta tables.                                 |
|                                                                                       |
|  [POWER BI SEMANTIC MODEL]                                                            |
|   - Direct Lake mode for memory-speed DAX analytics over OneLake.                     |
+---------------------------------------------------------------------------------------+
```

## Key Architectural Decision Records (ADRs)

1. **ADR-01: Storage Engine - Delta Lake on OneLake**
   - *Context*: Need transactional updates, history tracking, and fast columnar querying.
   - *Decision*: Standardize on Delta Lake format inside Fabric OneLake.
   - *Benefit*: ACID compliance, time-travel, zero-copy Direct Lake integration.

2. **ADR-02: Change Processing - Watermarked Delta MERGE**
   - *Context*: High-volume daily updates without creating duplicate records.
   - *Decision*: Implement PySpark Delta `MERGE` logic matching on natural business keys (`order_id`, `product_id`, `customer_id`).
   - *Benefit*: Efficient upserts, idempotent pipeline reruns.

3. **ADR-03: Dimension History - SCD Type 2 for Customers**
   - *Context*: Customer attributes (city, segment) change over time while historical order context must be preserved.
   - *Decision*: Track updates in `dim_customer` with surrogate key `customer_key`, `effective_date`, `expiry_date`, and `is_current`.
