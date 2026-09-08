# Fabric PySpark Notebook: 06_transform_orders
# Description: Cleanse, standardize, and validate raw_orders and raw_order_items into silver_orders and silver_order_items Delta tables.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, current_timestamp

def transform_orders_silver(spark):
    print("Reading raw_orders and raw_order_items from Bronze Lakehouse...")
    bronze_orders = spark.read.table("raw_orders")
    bronze_order_items = spark.read.table("raw_order_items")
    
    # 1. Transform Orders
    cleaned_orders = bronze_orders.filter(col("order_id").isNotNull()) \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("order_date", to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("order_status", trim(col("order_status"))) \
        .withColumn("shipping_address", trim(col("shipping_address"))) \
        .withColumn("payment_id", trim(col("payment_id"))) \
        .withColumn("total_amount", col("total_amount").cast("double")) \
        .withColumn("updated_timestamp", current_timestamp()) \
        .dropDuplicates(["order_id"])
        
    print("Writing to Silver table: silver_orders")
    cleaned_orders.write.format("delta").mode("overwrite").saveAsTable("silver_orders")
    
    # 2. Transform Order Items
    cleaned_items = bronze_order_items.filter(col("order_item_id").isNotNull() & (col("quantity") > 0)) \
        .withColumn("order_item_id", trim(col("order_item_id"))) \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("product_id", trim(col("product_id"))) \
        .withColumn("quantity", col("quantity").cast("integer")) \
        .withColumn("unit_price", col("unit_price").cast("double")) \
        .withColumn("discount", col("discount").cast("double")) \
        .withColumn("line_total", (col("quantity") * col("unit_price")) - col("discount")) \
        .withColumn("updated_timestamp", current_timestamp()) \
        .dropDuplicates(["order_item_id"])
        
    print("Writing to Silver table: silver_order_items")
    cleaned_items.write.format("delta").mode("overwrite").saveAsTable("silver_order_items")
    
    print(f"Transformed {cleaned_orders.count()} orders and {cleaned_items.count()} order items into Silver Delta tables.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TransformOrdersSilver").getOrCreate()
    transform_orders_silver(spark)
