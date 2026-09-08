# Fabric PySpark Notebook: 02_ingest_products
# Description: Ingest raw Products data (JSON) into Bronze_Lakehouse with metadata preservation.

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name

def ingest_products_to_bronze(spark, source_path, pipeline_run_id="RUN_LOCAL_001"):
    print(f"Reading raw products JSON from: {source_path}")
    
    raw_df = spark.read.option("multiline", "true").json(source_path)
    
    bronze_df = raw_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name()) \
        .withColumn("batch_id", lit("BATCH_20260908")) \
        .withColumn("pipeline_run_id", lit(pipeline_run_id)) \
        .withColumn("source_system", lit("PRODUCT_CATALOG_API"))
        
    print("Writing to Bronze Lakehouse table: raw_products")
    bronze_df.write.format("delta").mode("overwrite").saveAsTable("raw_products")
    print(f"Successfully ingested {bronze_df.count()} records to raw_products.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("IngestProductsBronze").getOrCreate()
    ingest_products_to_bronze(spark, "abfss://ECommerce-Lakehouse@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/Files/raw_products.json")
