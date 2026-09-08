# Fabric PySpark Notebook: 12_gold_aggregations
# Description: Pre-calculates high-level executive business aggregations for instant dashboard consumption.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, countDistinct, avg, round as spark_round

def build_gold_aggregations(spark):
    print("Building Gold pre-aggregated reporting summaries...")
    
    fact_sales = spark.read.table("fact_sales")
    dim_date = spark.read.table("dim_date")
    
    monthly_summary = fact_sales.alias("f") \
        .join(dim_date.alias("d"), col("f.date_key") == col("d.date_key"), "inner") \
        .groupBy("d.year", "d.month", "d.month_name") \
        .agg(
            spark_round(spark_sum("f.sales_amount"), 2).alias("total_revenue"),
            spark_round(spark_sum("f.cost_amount"), 2).alias("total_cost"),
            spark_round(spark_sum("f.profit_amount"), 2).alias("total_profit"),
            countDistinct("f.order_id").alias("total_orders"),
            spark_round(spark_sum("f.sales_amount") / countDistinct("f.order_id"), 2).alias("average_order_value")
        ) \
        .orderBy("d.year", "d.month")
        
    print("Writing Gold summary table: agg_monthly_sales")
    monthly_summary.write.format("delta").mode("overwrite").saveAsTable("agg_monthly_sales")
    
    monthly_summary.show()
    print("Pre-aggregations complete.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GoldAggregations").getOrCreate()
    build_gold_aggregations(spark)
