# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, BooleanType
from pyspark.sql.functions import current_timestamp, col, count, when, lit, round

# 1. Define your explicit StructType schema (prevents schema drift and enables Rescued Data)
fraud_csv_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("sender_account", StringType(), True),
    StructField("receiver_account", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("location", StringType(), True),
    StructField("device_used", StringType(), True),
    StructField("is_fraud", BooleanType(), True),
    StructField("fraud_type", StringType(), True),
    StructField("time_since_last_transaction", DoubleType(), True),
    StructField("spending_deviation_score", DoubleType(), True),
    StructField("velocity_score", DoubleType(), True),
    StructField("geo_anomaly_score", DoubleType(), True),
    StructField("payment_channel", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("device_hash", StringType(), True)
])

# 2. Define the DDL for your stats tracking table
spark.sql("""
    CREATE TABLE IF NOT EXISTS finance.bronze.data_load_stats (
        run_timestamp TIMESTAMP,
        batch_id LONG,
        total_rows LONG,
        rescued_rows LONG,
        clean_rows LONG,
        rescued_percentage DOUBLE,
        status STRING
    )
    USING DELTA
""")

# 3. Define the batch-processing function for incremental logging & writing
def process_bronze_batch(batch_df, batch_id):
   
    # 2. Add lineage and metadata attributes
    bronze_batch_df = (
      batch_df.withColumn("source_file_path", col("_metadata.file_path"))
      .withColumn("ingestion_time", current_timestamp())
      .withColumn("batch_id", lit(batch_id))
    )
    bronze_batch_df.cache()
    total_count = bronze_batch_df.count()
    
    if total_count > 0:
        # Calculate metrics for this specific micro-batch
        stats_row = bronze_batch_df.select(
            current_timestamp().alias("run_timestamp"),
            lit(batch_id).alias("batch_id"),
            lit(total_count).alias("total_rows"),
            count(when(col("_rescued_data").isNotNull(), 1)).alias("rescued_rows"),
            count(when(col("_rescued_data").isNull(), 1)).alias("clean_rows"),
            round(count(when(col("_rescued_data").isNotNull(), 1)) * 100.0 / total_count, 2).alias("rescued_percentage"),
            lit("SUCCESS").alias("status")
        ).collect()[0]
        
        # Write clean raw data to the main Bronze Delta table
        bronze_batch_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("finance.bronze.transactions_raw")
            
        # Write batch metrics to your stats table
        spark.createDataFrame([stats_row]).write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("finance.bronze.data_load_stats")
            
        print(f"Batch {batch_id}: Processed {total_count} rows. Metrics logged.")
        
    bronze_batch_df.unpersist()

# 4. Setup Auto Loader stream with explicit schema
raw_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(fraud_csv_schema)
    .option("cloudFiles.schemaLocation", "/Volumes/finance/bronze/operational_data_vol/metadata/_schema_inference/transactions_raw") # <--- Still required!
    .option("rescuedDataColumn", "_rescued_data")
    .load("/Volumes/finance/bronze/operational_data_vol/finance/")
)

# 5. Execute the stream using foreachBatch
bronze_query = (
    raw_stream_df.writeStream
    .foreachBatch(process_bronze_batch)
    .option("checkpointLocation", "/Volumes/finance/bronze/operational_data_vol/metadata/_checkpoints/transactions_raw")
    .trigger(availableNow=True)
    .start()
)

bronze_query.awaitTermination()
print("Bronze incremental pipeline with explicit schema complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Execution Behavior: Why `.cache()` 
# MAGIC
# MAGIC ### What Happens When You Have 3 Separate Actions Without `.cache()`?
# MAGIC
# MAGIC Because you called three actions in a row, Spark processes the data redundantly:
# MAGIC
# MAGIC * **Action 1 (`.count()`):** $\rightarrow$ Spark executes the recipe (lineage) and reads the files from storage to calculate the total row count.
# MAGIC * **Action 2 (`.collect()`):** $\rightarrow$ Spark forgets the previous results, re-runs the entire recipe from scratch, and reads the files again to calculate the statistics.
# MAGIC * **Action 3 (`.saveAsTable()`):** $\rightarrow$ Spark forgets the results again, re-runs the recipe a third time, and reads the files a third time from storage to write the records into your Delta table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Role of `.collect()` in Caching
# MAGIC
# MAGIC Even though `.collect()` is an action that triggers execution, it **only pulls its specific result back to your driver node**. It does *not* automatically save or retain the DataFrame in the cluster's memory for subsequent `.write()` or `.count()` operations.
# MAGIC
# MAGIC Because you have three separate actions (`count`, `collect`, and `write`), Spark treats them as three independent requests and processes the data **three separate times**—unless `.cache()` is explicitly called to store the DataFrame in RAM on the first run and hold it there for the remaining two.