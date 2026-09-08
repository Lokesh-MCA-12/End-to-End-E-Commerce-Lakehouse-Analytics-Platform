# Fabric PySpark Notebook: 11_incremental_processing
# Description: Demonstrates Change Data Capture (CDC) with Watermarked Extraction and Delta Lake MERGE upserts.

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

def execute_incremental_merge(spark):
    print("Executing Incremental Batch Delta MERGE process...")
    
    # 1. Simulate watermark timestamp extraction
    last_watermark = "2026-01-01 00:00:00"
    print(f"Reading new/updated orders modified since watermark: {last_watermark}")
    
    incremental_orders = spark.read.table("raw_orders") \
        .filter(f"ingestion_timestamp >= '{last_watermark}'")
        
    print(f"Extracted {incremental_orders.count()} incremental order records.")
    
    # 2. Perform Delta MERGE upsert into Silver orders
    target_delta = DeltaTable.forName(spark, "silver_orders")
    
    target_delta.alias("target").merge(
        incremental_orders.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdate(set={
        "customer_id": "source.customer_id",
        "order_date": "source.order_date",
        "order_status": "source.order_status",
        "total_amount": "source.total_amount",
        "updated_timestamp": "current_timestamp()"
    }).whenNotMatchedInsert(values={
        "order_id": "source.order_id",
        "customer_id": "source.customer_id",
        "order_date": "source.order_date",
        "order_status": "source.order_status",
        "shipping_address": "source.shipping_address",
        "payment_id": "source.payment_id",
        "total_amount": "source.total_amount",
        "updated_timestamp": "current_timestamp()"
    }).execute()
    
    print("Delta MERGE upsert execution successfully completed.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("IncrementalDeltaMerge").getOrCreate()
    execute_incremental_merge(spark)
