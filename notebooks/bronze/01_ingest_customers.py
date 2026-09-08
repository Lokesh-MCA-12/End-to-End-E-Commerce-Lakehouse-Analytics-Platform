# Fabric PySpark Notebook: 01_ingest_customers
# Description: Ingest raw Customer data (CSV) into Bronze_Lakehouse with metadata preservation.

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name

def ingest_customers_to_bronze(spark, source_path, pipeline_run_id="RUN_LOCAL_001"):
    print(f"Reading raw customers CSV from: {source_path}")
    
    raw_df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
    
    # Enrich with operational ingestion metadata
    bronze_df = raw_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name()) \
        .withColumn("batch_id", lit("BATCH_20260908")) \
        .withColumn("pipeline_run_id", lit(pipeline_run_id)) \
        .withColumn("source_system", lit("WEB_CRM_DB"))
        
    print("Writing to Bronze Lakehouse table: raw_customers")
    bronze_df.write.format("delta").mode("overwrite").saveAsTable("raw_customers")
    print(f"Successfully ingested {bronze_df.count()} records to raw_customers.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("IngestCustomersBronze").getOrCreate()
    ingest_customers_to_bronze(spark, "abfss://ECommerce-Lakehouse@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/Files/raw_customers.csv")
