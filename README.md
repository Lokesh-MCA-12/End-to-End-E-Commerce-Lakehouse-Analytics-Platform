# End-to-End E-Commerce Lakehouse Analytics Platform

A real-time, enterprise-grade cloud data engineering and lakehouse analytics solution designed for multi-source e-commerce operational intelligence. Built natively on **Microsoft Fabric** using **OneLake**, **Delta Lake**, **PySpark**, **T-SQL Analytics Endpoint**, and **Power BI (Direct Lake Mode)**, accompanied by a complete local **Python + DuckDB simulation & REST API interactive operational web dashboard**.

![Microsoft Fabric](https://img.shields.io/badge/Platform-Microsoft%20Fabric-0078D4?style=for-the-badge&logo=microsoft)
![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze--Silver--Gold)-FFB900?style=for-the-badge)
![Storage](https://img.shields.io/badge/Storage-Delta%20Lake%20%7C%20OneLake-008080?style=for-the-badge&logo=apachespark)
![Engine](https://img.shields.io/badge/Engine-PySpark%20%7C%20DuckDB%20%7C%20T--SQL-E25A1C?style=for-the-badge&logo=apachespark)
![BI](https://img.shields.io/badge/BI-Power%20BI%20Direct%20Lake-F2C811?style=for-the-badge&logo=powerbi)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

---

## 🚀 Key Features

1. **Medallion Architecture Pipeline**: Multi-layer operational pipeline with strict data isolation across **Bronze** (raw landing & metadata preservation), **Silver** (cleansed, validated & standardized Delta tables), and **Gold** (analytical Star Schema facts & dimensions).
2. **SCD Type 2 Customer Tracking**: Advanced Slowly Changing Dimension (SCD Type 2) implementation preserving full historical context for customer attributes using surrogate keys, effective dates, expiry dates, and current flags.
3. **Automated Data Quality Governance**: Automated data contract enforcement validating null checks, schema constraints, range assertions, and referential integrity with real-time audit logging to `data_quality_results`.
4. **Direct Lake & T-SQL Analytics Layer**: Serverless T-SQL analytics endpoint querying OneLake Delta tables coupled with memory-speed Power BI Direct Lake mode reporting eliminating ETL refresh delays.
5. **Interactive Dashboard & Local Simulation**: Includes `run_local_pipeline_simulation.py` and `server.py` allowing instant local execution, data generation, Medallion ETL processing, and live REST API telemetry web visualization on `http://localhost:8080`.

---

## 🛠️ Technology Stack

* **Cloud Platform & Lakehouse**: Microsoft Fabric, OneLake, Delta Lake format.
* **Data Processing & Orchestration**: PySpark, Fabric Data Factory (`PL_Ecommerce_Ingestion`), Python 3, DuckDB.
* **Query & Data Governance Engine**: Fabric Serverless T-SQL Analytics Endpoint, SQL, Data Quality Gateways.
* **BI & Interactive Telemetry**: Power BI (Direct Lake Semantic Model), HTML5 / Modern CSS3 / JavaScript (REST API Dashboard).

---

## 🏗️ Architecture & Data Flow

```
+---------------------------------------------------------------------------------------------------+
|                                     E-COMMERCE DATA SOURCES                                       |
|  [ Web/Mobile Logs ]     [ Operational SQL DB ]     [ Payment API ]     [ ERP & Inventory Systems ] |
+---------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
|                                  MICROSOFT FABRIC WORKSPACE                                       |
|                                                                                                   |
|  Fabric Data Pipeline: PL_Ecommerce_Ingestion                                                    |
|    |                                                                                              |
|    +--> BRONZE LAKEHOUSE (Raw Landing & Operational Metadata Preservation)                        |
|    |      - raw_customers, raw_products, raw_orders, raw_order_items, raw_payments, etc.        |
|    |                                                                                              |
|    +--> SILVER LAKEHOUSE (PySpark Cleansing, Schema Validation & Standardization)                 |
|    |      - silver_customers, silver_products, silver_orders, silver_order_items, etc.          |
|    |                                                                                              |
|    +--> GOLD LAKEHOUSE (Star Schema & Dimensional Modeling - Delta Lake)                          |
|           - Fact Tables: fact_sales, fact_returns, fact_inventory                                 |
|           - Dimension Tables: dim_customer (SCD2), dim_product, dim_date, dim_category            |
|                                                                                                   |
|  Fabric SQL Analytics Endpoint (Serverless T-SQL Analytics Engine)                               |
|  Power BI Semantic Model (Direct Lake Mode on OneLake)                                            |
+---------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
|                            POWER BI & REST API ANALYTICAL DASHBOARDS                             |
|  [ Executive Overview ]   [ Product Analytics ]   [ Customer Intelligence ]   [ Operations & Inventory ] |
+---------------------------------------------------------------------------------------------------+
```

---

## ⚙️ How to Install and Run

### Prerequisites
* **Python 3.8+** installed on your system.
* A modern web browser (Chrome, Edge, Firefox).

### 1. Clone the Repository
```bash
git clone https://github.com/Lokesh-MCA-12/End-to-End-E-Commerce-Lakehouse-Analytics-Platform.git
cd End-to-End-E-Commerce-Lakehouse-Analytics-Platform
```

### 2. Run the Local Pipeline Simulation
Execute the standalone Python + DuckDB Medallion Lakehouse pipeline script to generate raw synthetic datasets, cleanse records, build Gold Star Schema models, enforce data quality rules, and execute analytical SQL queries:
```bash
python run_local_pipeline_simulation.py
```

### 3. Launch the Interactive Web Dashboard Server
Start the HTTP REST API server & static asset host:
```bash
python server.py
```
Open your browser and navigate to:
👉 **[http://localhost:8080](http://localhost:8080)**

---

## 🗄️ Project Structure & Folder Map

```
End-to-End-E-Commerce-Lakehouse-Analytics-Platform/
├── README.md                           # Master Technical & Architectural Overview
├── run_local_pipeline_simulation.py    # Local Python/DuckDB Medallion Pipeline Simulator
├── server.py                           # Full-Stack HTTP REST API & Telemetry Web Server
├── .gitignore                          # Version Control exclusions
├── architecture/                       # Deep-dive architectural specs & ERD diagrams
│   └── architecture_overview.md
├── datasets/                           # Synthetic data generation & domain schema generators
│   └── generate_datasets.py
├── notebooks/                          # Production Microsoft Fabric PySpark Notebooks
│   ├── bronze/                         # 01 to 03: Raw data landing & ingestion
│   ├── silver/                         # 04 to 07: Data cleansing, deduplication & standardization
│   └── gold/                           # 08 to 12: Star schema facts & SCD Type 2 dimension building
├── sql/                                # Production T-SQL analytics scripts & DDLs
│   ├── dimensions/                     # Dimension table definition DDLs
│   ├── facts/                          # Fact table definition DDLs
│   ├── analytics/                      # Analytical T-SQL reporting queries
│   └── 05_data_quality_governance_rules.sql
├── pipelines/                          # Microsoft Fabric Data Factory pipeline definitions (JSON)
│   └── PL_Ecommerce_Ingestion.json
├── powerbi/                            # DAX measure library & Direct Lake semantic model specs
│   ├── dax_measures.dax
│   └── semantic_model_design.md
├── public/                             # Interactive Web Dashboard UI Assets
│   ├── index.html                      # Lakehouse Operational Dashboard interface
│   ├── app.js                         # Telemetry engine & REST API integration logic
│   └── styles.css                     # Modern dark-mode custom UI design system
└── documentation/                     # Deployment guides & executive narrative summaries
    ├── fabric_lakehouse_deployment_guide.md
    ├── technical_implementation_guide.md
    └── interview_narrative_and_checklist.md
```

---

## 📊 Data Schema & Star Schema Dimensional Model

### Domain Datasets (7 Core Entities)
1. **Customers**: `customer_id`, `first_name`, `last_name`, `email`, `gender`, `city`, `state`, `country`, `registration_date`, `customer_segment`.
2. **Products**: `product_id`, `product_name`, `category_id`, `category_name`, `brand`, `unit_price`, `cost_price`, `supplier`.
3. **Orders**: `order_id`, `customer_id`, `order_date`, `order_status`, `shipping_address`, `payment_id`, `total_amount`.
4. **Order Items**: `order_item_id`, `order_id`, `product_id`, `quantity`, `unit_price`, `discount`.
5. **Payments**: `payment_id`, `order_id`, `payment_method`, `payment_status`, `payment_amount`, `payment_date`.
6. **Returns**: `return_id`, `order_id`, `product_id`, `return_date`, `return_reason`, `refund_amount`.
7. **Inventory**: `inventory_id`, `product_id`, `warehouse_id`, `stock_quantity`, `inventory_date`.

### Gold Layer Star Schema Architecture
* **`fact_sales`**: Line-item level order granular fact table. Includes metrics: `quantity`, `unit_price`, `discount`, `sales_amount`, `cost_amount`, `profit_amount`.
* **`dim_customer`**: **SCD Type 2** tracking customer profile updates historically over time (`effective_date`, `expiry_date`, `is_current`).
* **`dim_product`**: Product catalog metadata, brand categorization, and baseline price/cost indices.
* **`dim_date`**: Calendar date dimension for time-intelligence reporting (`date_key`, `full_date`, `year`, `quarter`, `month`, `week`, `is_weekend`).

---

## 💼 Executive & Technical Interview Narrative

> "I built an enterprise end-to-end e-commerce lakehouse platform on Microsoft Fabric. The platform ingests multi-source transactional data via Fabric Data Factory pipelines into a Bronze raw storage layer, utilizes PySpark notebooks to clean, validate, and standardize records into Silver Delta tables, and constructs a Gold Star Schema layer featuring SCD Type 2 historical customer tracking. The system enforces automated data quality governance gates, leverages serverless T-SQL for analytics, and connects directly to Power BI via Direct Lake mode for zero-latency interactive reporting."

---

## 📜 License

This project is licensed under the [MIT License](LICENSE).
