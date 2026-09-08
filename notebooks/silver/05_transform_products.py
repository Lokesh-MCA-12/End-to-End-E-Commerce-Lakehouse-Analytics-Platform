# Fabric PySpark Notebook: 05_transform_products
# Description: Cleanse, standardize, and validate raw_products into silver_products Delta table.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_timestamp

def transform_products_silver(spark):
    print("Reading raw_products from Bronze Lakehouse...")
    bronze_products = spark.read.table("raw_products")
    
    # Validation: Filter out null product IDs or non-positive unit prices
    cleaned_df = bronze_products.filter(
        col("product_id").isNotNull() & 
        (col("unit_price") > 0)
    )
    
    silver_products = cleaned_df \
        .withColumn("product_id", trim(col("product_id"))) \
        .withColumn("product_name", trim(col("product_name"))) \
        .withColumn("category_id", col("category_id").cast("integer")) \
        .withColumn("category_name", trim(col("category_name"))) \
        .withColumn("brand", trim(col("brand"))) \
        .withColumn("unit_price", col("unit_price").cast("double")) \
        .withColumn("cost_price", col("cost_price").cast("double")) \
        .withColumn("supplier", trim(col("supplier"))) \
        .withColumn("profit_margin_amt", col("unit_price") - col("cost_price")) \
        .withColumn("updated_timestamp", current_timestamp()) \
        .dropDuplicates(["product_id"])
        
    print("Writing cleansed data to Silver Lakehouse table: silver_products")
    silver_products.write.format("delta").mode("overwrite").saveAsTable("silver_products")
    print(f"Successfully transformed {silver_products.count()} records into silver_products Delta table.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TransformProductsSilver").getOrCreate()
    transform_products_silver(spark)
