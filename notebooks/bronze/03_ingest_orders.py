# Fabric PySpark Notebook: 03_ingest_orders
# Description: Ingest raw Orders, Order Items, Payments, Returns, and Inventory datasets into Bronze_Lakehouse.

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name

def ingest_orders_entities_to_bronze(spark, base_dir, pipeline_run_id="RUN_LOCAL_001"):
    entities = [
        ("raw_orders.csv", "raw_orders", "csv", "ORDER_MANAGEMENT_SYS"),
        ("raw_order_items.csv", "raw_order_items", "csv", "ORDER_MANAGEMENT_SYS"),
        ("raw_payments.json", "raw_payments", "json", "PAYMENT_GATEWAY_API"),
        ("raw_returns.csv", "raw_returns", "csv", "CUSTOMER_SERVICE_APP"),
        ("raw_inventory.csv", "raw_inventory", "csv", "WAREHOUSE_ERP")
    ]
    
    for filename, table_name, file_type, source_sys in entities:
        source_path = f"{base_dir}/{filename}"
        print(f"Reading {filename} ({file_type}) for {table_name}...")
        
        if file_type == "csv":
            raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
        else:
            raw_df = spark.read.option("multiline", "true").json(source_path)
            
        bronze_df = raw_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", input_file_name()) \
            .withColumn("batch_id", lit("BATCH_20260908")) \
            .withColumn("pipeline_run_id", lit(pipeline_run_id)) \
            .withColumn("source_system", lit(source_sys))
            
        print(f"Writing to Bronze Lakehouse table: {table_name}")
        bronze_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Ingested {bronze_df.count()} records to {table_name}.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("IngestOrdersEntitiesBronze").getOrCreate()
    ingest_orders_entities_to_bronze(spark, "abfss://ECommerce-Lakehouse@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/Files")
