# Fabric PySpark Notebook: 10_data_quality
# Description: Automated Data Quality Governance Framework logging test metrics to data_quality_results table.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, count, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

def run_data_quality_checks(spark):
    print("Executing Automated Data Quality Validation Framework...")
    
    results = []
    
    # Validation 1: Customer Null Keys in Silver
    silver_cust = spark.read.table("silver_customers")
    total_cust = silver_cust.count()
    failed_cust_keys = silver_cust.filter(col("customer_id").isNull()).count()
    success_pct_cust = round(((total_cust - failed_cust_keys) / total_cust) * 100, 2) if total_cust > 0 else 100.0
    status_cust = "PASSED" if failed_cust_keys == 0 else "FAILED"
    
    results.append(("silver_customers", "check_null_customer_id", total_cust, failed_cust_keys, success_pct_cust, status_cust))
    
    # Validation 2: Duplicate Orders in Silver
    silver_orders = spark.read.table("silver_orders")
    total_orders = silver_orders.count()
    dup_orders = silver_orders.groupBy("order_id").count().filter(col("count") > 1).count()
    success_pct_ord = round(((total_orders - dup_orders) / total_orders) * 100, 2) if total_orders > 0 else 100.0
    status_ord = "PASSED" if dup_orders == 0 else "FAILED"
    
    results.append(("silver_orders", "check_duplicate_order_id", total_orders, dup_orders, success_pct_ord, status_ord))
    
    # Validation 3: Non-negative Quantity in Order Items
    order_items = spark.read.table("silver_order_items")
    total_items = order_items.count()
    invalid_qty = order_items.filter(col("quantity") <= 0).count()
    success_pct_qty = round(((total_items - invalid_qty) / total_items) * 100, 2) if total_items > 0 else 100.0
    status_qty = "PASSED" if invalid_qty == 0 else "WARNING"
    
    results.append(("silver_order_items", "check_positive_quantity", total_items, invalid_qty, success_pct_qty, status_qty))
    
    # Save results to Lakehouse table data_quality_results
    schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("check_name", StringType(), True),
        StructField("total_records", LongType(), True),
        StructField("failed_records", LongType(), True),
        StructField("success_percentage", DoubleType(), True),
        StructField("status", StringType(), True)
    ])
    
    results_df = spark.createDataFrame(results, schema).withColumn("execution_timestamp", current_timestamp())
    print("Writing Data Quality Audit Log to: data_quality_results")
    results_df.write.format("delta").mode("append").saveAsTable("data_quality_results")
    
    results_df.show(truncate=False)
    print("Data Quality Evaluation complete.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataQualityFramework").getOrCreate()
    run_data_quality_checks(spark)
