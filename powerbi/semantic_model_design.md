# Power BI Direct Lake Semantic Model Design

## Overview
The semantic model is constructed over the **Gold_Lakehouse** tables using **Direct Lake** mode in Microsoft Fabric. Direct Lake reads Delta table Parquet data files directly from **OneLake** into memory, providing DirectQuery-level freshness with Import-level analytical speed.

## Entity Relationships (Star Schema)

```
       +-------------------+
       |    dim_customer   |
       | (customer_key PK) |
       +-------------------+
                 |
                 | 1:N (Single Direction)
                 v
+----------------+----------------+       +-------------------+
|            fact_sales           | ----> |    dim_product    |
| (customer_key, product_key, FK) | 1:N   | (product_key PK)  |
+----------------+----------------+       +-------------------+
                 |
                 | 1:N (Single Direction)
                 v
       +-------------------+
       |      dim_date     |
       |   (date_key PK)   |
       +-------------------+
```

## Key Configuration Requirements
1. **Relationship Cardinality**:
   - `dim_customer[customer_key]` (1) $\rightarrow$ `fact_sales[customer_key]` (N)
   - `dim_product[product_key]` (1) $\rightarrow$ `fact_sales[product_key]` (N)
   - `dim_date[date_key]` (1) $\rightarrow$ `fact_sales[date_key]` (N)
2. **Cross Filtering**: Single direction (Dimension to Fact) to ensure predictable DAX filter context propagation.
3. **Data Types**: Enforce integer surrogates for keys to maximize VertiPaq column compression.
