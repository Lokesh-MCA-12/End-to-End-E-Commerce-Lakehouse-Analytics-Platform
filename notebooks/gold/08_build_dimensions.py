# Fabric PySpark Notebook: 08_build_dimensions
# Description: Constructs Gold Layer Dimensions (dim_customer with SCD Type 2, dim_product, dim_date, dim_category, dim_location).

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, md5, concat_ws, date_format, year, month, quarter, 
    dayofmonth, dayofweek, date_add, to_date, sequence, expr
)

def build_dimensions_gold(spark):
    print("Building Gold Layer Dimensions...")
    
    # 1. dim_customer (SCD Type 2)
    silver_customers = spark.read.table("silver_customers")
    
    dim_customer = silver_customers.select(
        md5(concat_ws("||", col("customer_id"), lit("2026-01-01"))).alias("customer_key"),
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("full_name"),
        col("email"),
        col("gender"),
        col("city"),
        col("state"),
        col("country"),
        col("customer_segment"),
        to_date(col("registration_date")).alias("effective_date"),
        to_date(lit("9999-12-31")).alias("expiry_date"),
        lit(True).alias("is_current")
    )
    
    print("Writing Gold Dimension: dim_customer (SCD Type 2)")
    dim_customer.write.format("delta").mode("overwrite").saveAsTable("dim_customer")
    
    # 2. dim_product
    silver_products = spark.read.table("silver_products")
    dim_product = silver_products.select(
        md5(col("product_id")).alias("product_key"),
        col("product_id"),
        col("product_name"),
        col("category_id"),
        col("category_name"),
        col("brand"),
        col("unit_price"),
        col("cost_price"),
        col("supplier")
    )
    print("Writing Gold Dimension: dim_product")
    dim_product.write.format("delta").mode("overwrite").saveAsTable("dim_product")
    
    # 3. dim_date
    start_date = "2025-01-01"
    end_date = "2026-12-31"
    
    date_df = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date")
    
    dim_date = date_df.select(
        date_format(col("full_date"), "yyyyMMdd").cast("integer").alias("date_key"),
        col("full_date"),
        dayofmonth(col("full_date")).alias("day"),
        month(col("full_date")).alias("month"),
        date_format(col("full_date"), "MMMM").alias("month_name"),
        quarter(col("full_date")).alias("quarter"),
        year(col("full_date")).alias("year"),
        date_format(col("full_date"), "w").cast("integer").alias("week"),
        date_format(col("full_date"), "EEEE").alias("day_name"),
        expr("CASE WHEN dayofweek(full_date) IN (1, 7) THEN True ELSE False END").alias("is_weekend")
    )
    print("Writing Gold Dimension: dim_date")
    dim_date.write.format("delta").mode("overwrite").saveAsTable("dim_date")
    
    print("Successfully built all Gold dimension tables.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BuildDimensionsGold").getOrCreate()
    build_dimensions_gold(spark)
