# Interview Narrative, Checklist & Portfolio Guide

## Technical Elevator Pitch (Interview Narrative)

> "I designed and deployed an end-to-end e-commerce data platform in Microsoft Fabric using a Medallion Lakehouse Architecture. I built orchestrated Data Factory pipelines to land heterogeneous raw source data (orders, items, products, customers, payments, returns, inventory) into a Bronze lakehouse, enriching records with operational lineage metadata. 
> 
> Using PySpark, I cleansed, validated, and integrated these raw datasets into Silver Delta tables, enforcing strict schema constraints and deduplication rules. For the Gold layer, I constructed a star schema with SCD Type 2 dimension tracking for customer history and created central fact tables for sales and returns. 
> 
> I enabled business analytics through serverless T-SQL via the Fabric SQL analytics endpoint and exposed curated metrics to Power BI dashboards using Direct Lake mode for memory-speed reporting. The platform features watermarked incremental MERGE processing, an automated data quality audit framework, and pipeline execution logging."

---

## Final Technical Checklist

- [x] **Architecture**: Bronze / Silver / Gold Medallion boundaries strictly defined and implemented.
- [x] **Ingestion**: Raw landing zone enriches records with `ingestion_timestamp`, `source_file`, `batch_id`, `pipeline_run_id`, `source_system`.
- [x] **PySpark Transformation**: Modular notebooks perform type-casting, string trimming, null handling, deduplication, and integration.
- [x] **Delta Lake Features**: Delta format utilized; MERGE upsert pattern demonstrated for Change Data Capture.
- [x] **Dimensional Modeling**: Star schema with `fact_sales`, `fact_returns`, `dim_customer` (SCD Type 2), `dim_product`, `dim_date`.
- [x] **Data Quality Governance**: Automated testing logs pass/fail metrics and percentages to `data_quality_results`.
- [x] **SQL Analytics**: Serverless T-SQL queries implemented for revenue velocity, top products, customer segments, and operations metrics.
- [x] **Power BI Integration**: Direct Lake semantic model specification and explicit DAX measures authored.
- [x] **Verification**: Local Python + DuckDB simulation runner verifies execution, data quality, and T-SQL analytics outputs end-to-end.
