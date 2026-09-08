# Fabric PySpark Notebook: 04_transform_customers
# Description: Cleanse, standardize, validate, and deduplicate raw_customers into silver_customers Delta table.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_date, when, current_timestamp

def transform_customers_silver(spark):
    print("Reading raw_customers from Bronze Lakehouse...")
    bronze_customers = spark.read.table("raw_customers")
    
    # 1. Null handling & Primary Key validation
    cleaned_df = bronze_customers.filter(col("customer_id").isNotNull() & (trim(col("customer_id")) != ""))
    
    # 2. String standardization & Trim whitespaces
    silver_df = cleaned_df \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("first_name", trim(col("first_name"))) \
        .withColumn("last_name", trim(col("last_name"))) \
        .withColumn("full_name", trim(col("first_name")) + " " + trim(col("last_name"))) \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("gender", trim(col("gender"))) \
        .withColumn("city", trim(col("city"))) \
        .withColumn("state", trim(col("state"))) \
        .withColumn("country", trim(col("country"))) \
        .withColumn("registration_date", to_date(col("registration_date"), "yyyy-MM-dd")) \
        .withColumn("customer_segment", trim(col("customer_segment"))) \
        .withColumn("updated_timestamp", current_timestamp())
        
    # 3. Deduplication based on customer_id
    dedup_df = silver_df.dropDuplicates(["customer_id"])
    
    # Select standardized Silver schema
    silver_customers = dedup_df.select(
        "customer_id", "first_name", "last_name", "full_name", "email",
        "gender", "city", "state", "country", "registration_date",
        "customer_segment", "updated_timestamp", "ingestion_timestamp", "source_system"
    )
    
    print("Writing cleansed data to Silver Lakehouse table: silver_customers")
    silver_customers.write.format("delta").mode("overwrite").saveAsTable("silver_customers")
    print(f"Successfully transformed {silver_customers.count()} records into silver_customers Delta table.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TransformCustomersSilver").getOrCreate()
    transform_customers_silver(spark)
