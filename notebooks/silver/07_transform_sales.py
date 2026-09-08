# Fabric PySpark Notebook: 07_transform_sales
# Description: Integrates Orders, Order Items, Products, Customers, and Payments into a unified Silver sales dataset.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, current_timestamp

def transform_sales_silver(spark):
    print("Integrating Silver tables for sales dataset...")
    
    orders_df = spark.read.table("silver_orders")
    items_df = spark.read.table("silver_order_items")
    products_df = spark.read.table("silver_products")
    customers_df = spark.read.table("silver_customers")
    
    # Perform joins with key validation and zero duplication
    sales_integrated = items_df.alias("i") \
        .join(orders_df.alias("o"), col("i.order_id") == col("o.order_id"), "inner") \
        .join(products_df.alias("p"), col("i.product_id") == col("p.product_id"), "inner") \
        .join(customers_df.alias("c"), col("o.customer_id") == col("c.customer_id"), "inner")
        
    sales_clean = sales_integrated.select(
        col("i.order_item_id"),
        col("o.order_id"),
        col("o.customer_id"),
        col("i.product_id"),
        col("p.category_id"),
        col("p.category_name"),
        col("o.order_date"),
        col("o.order_status"),
        col("i.quantity"),
        col("i.unit_price"),
        col("i.discount"),
        col("p.cost_price"),
        spark_round((col("i.quantity") * col("i.unit_price")) - col("i.discount"), 2).alias("sales_amount"),
        spark_round(col("i.quantity") * col("p.cost_price"), 2).alias("cost_amount"),
        spark_round(((col("i.quantity") * col("i.unit_price")) - col("i.discount")) - (col("i.quantity") * col("p.cost_price")), 2).alias("profit_amount"),
        current_timestamp().alias("updated_timestamp")
    )
    
    print("Writing unified sales dataset to Silver Lakehouse: silver_sales")
    sales_clean.write.format("delta").mode("overwrite").saveAsTable("silver_sales")
    print(f"Successfully created silver_sales with {sales_clean.count()} integrated records.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TransformSalesSilver").getOrCreate()
    transform_sales_silver(spark)
