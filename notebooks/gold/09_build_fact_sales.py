# Fabric PySpark Notebook: 09_build_fact_sales
# Description: Constructs Gold Layer Fact Tables (fact_sales, fact_returns, fact_inventory) using Star Schema surrogate keys.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, date_format, to_date

def build_fact_tables_gold(spark):
    print("Building Gold Layer Fact Tables...")
    
    silver_sales = spark.read.table("silver_sales")
    dim_customer = spark.read.table("dim_customer").filter(col("is_current") == True)
    dim_product = spark.read.table("dim_product")
    
    # 1. fact_sales
    fact_sales = silver_sales.alias("s") \
        .join(dim_customer.alias("c"), col("s.customer_id") == col("c.customer_id"), "left") \
        .join(dim_product.alias("p"), col("s.product_id") == col("p.product_id"), "left") \
        .select(
            md5(col("s.order_item_id")).alias("sales_key"),
            col("s.order_id"),
            col("c.customer_key"),
            col("p.product_key"),
            date_format(to_date(col("s.order_date")), "yyyyMMdd").cast("integer").alias("date_key"),
            col("s.quantity"),
            col("s.unit_price"),
            col("s.discount"),
            col("s.sales_amount"),
            col("s.cost_amount"),
            col("s.profit_amount")
        )
        
    print("Writing Gold Fact Table: fact_sales")
    fact_sales.write.format("delta").mode("overwrite").saveAsTable("fact_sales")
    
    # 2. fact_returns
    silver_returns = spark.read.table("silver_returns") if spark.catalog.tableExists("silver_returns") else spark.read.table("raw_returns")
    fact_returns = silver_returns.alias("r") \
        .join(dim_product.alias("p"), col("r.product_id") == col("p.product_id"), "left") \
        .select(
            md5(col("r.return_id")).alias("return_key"),
            col("r.return_id"),
            col("r.order_id"),
            col("p.product_key"),
            date_format(to_date(col("r.return_date")), "yyyyMMdd").cast("integer").alias("date_key"),
            col("r.return_reason"),
            col("r.refund_amount").cast("double").alias("refund_amount")
        )
    print("Writing Gold Fact Table: fact_returns")
    fact_returns.write.format("delta").mode("overwrite").saveAsTable("fact_returns")
    
    print("Successfully created Gold Star Schema Fact tables.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BuildFactTablesGold").getOrCreate()
    build_fact_tables_gold(spark)
