# Databricks notebook source
# Create a dropdown widget for behavior mode
dbutils.widgets.dropdown(
    name="behavior_mode",
    defaultValue="HARD_STOP",
    choices=["HARD_STOP", "QUARANTINE", "WARNING_LOAD"],
    label="Quality Failure Behavior",
)

# Retrieve the selected parameter value
behavior_mode = dbutils.widgets.text("behavior_mode","HARD_STOP")
behavior_mode = dbutils.widgets.get("behavior_mode")
print(f"Pipeline running in mode: {behavior_mode}")

# COMMAND ----------

# ==========================================
# CELL 2: Batch Processing Function
# ==========================================
from pyspark.sql.functions import col, current_timestamp, lit, when


def process_silver_batch(batch_df, batch_id):
  # 1. Cache the incoming micro-batch for multiple actions
  batch_df.cache()

  # 2. Apply core transformations, watermarking, deduplication, and risk scoring
  raw_processed_df = (
      batch_df.withWatermark("timestamp", "10 minutes")
      .dropDuplicates(["transaction_id"])
      .withColumn("processed_at", current_timestamp())
      .withColumn(
          "initial_risk_tier",
          when(
              (col("velocity_score") > 80) | (col("geo_anomaly_score") > 80),
              lit("HIGH"),
          )
          .when(
              (col("velocity_score") > 50) | (col("geo_anomaly_score") > 50),
              lit("MEDIUM"),
          )
          .otherwise(lit("LOW")),
      )
  )

  # Explicitly align column order to match the exact Silver Delta table schema and Cluster By definition
  processed_silver_df = raw_processed_df.select(
      "transaction_id",
      "timestamp",
      "sender_account",
      "receiver_account",
      "amount",
      "transaction_type",
      "merchant_category",
      "location",
      "device_used",
      "is_fraud",
      "fraud_type",
      "time_since_last_transaction",
      "spending_deviation_score",
      "velocity_score",
      "geo_anomaly_score",
      "payment_channel",
      "ip_address",
      "device_hash",
      "processed_at",
      "initial_risk_tier",
      "source_file_path",
      "ingestion_time",
      "batch_id",
  )

  # 3. Calculate Quality Metrics for this batch
  total_records = processed_silver_df.count()
  invalid_amounts = processed_silver_df.filter(
      (col("amount") <= 0) | col("amount").isNull()
  ).count()
  null_ids = processed_silver_df.filter(
      col("transaction_id").isNull() | col("sender_account").isNull()
  ).count()

  status = "PASS"
  if invalid_amounts > 0 or null_ids > 0:
    status = "FAILED"

  quarantined_count = (
      processed_silver_df.filter(
          (col("amount") <= 0)
          | col("amount").isNull()
          | col("transaction_id").isNull()
          | col("sender_account").isNull()
      ).count()
      if status == "FAILED"
      else 0
  )

  # 4. Log metrics to audit table using a safe SQL select approach (auto-creates table via Delta)
  metrics_df = spark.sql(f"""
        SELECT 
            current_timestamp() as run_timestamp,
            CAST({batch_id} AS INT) as batch_id,
            CAST({total_records} AS INT) as total_records,
            CAST({invalid_amounts} AS INT) as invalid_amount_count,
            CAST({null_ids} AS INT) as null_id_count,
            CAST({quarantined_count} AS INT) as quarantined_count,
            '{status}' as quality_status,
            '{behavior_mode}' as execution_mode
    """)

  metrics_df.write.format("delta").mode("append").saveAsTable(
      "finance.silver.quality_metrics"
  )

  # 5. Branching logic based on parameter mode if quality check fails
  if status == "FAILED":

    if behavior_mode == "HARD_STOP":
      batch_df.unpersist()
      raise ValueError(
          f"[HARD_STOP] Batch {batch_id} failed quality checks! "
          f"Quarantined/Bad Records: {quarantined_count}. Pipeline aborted."
      )

    elif behavior_mode == "QUARANTINE":
      print(
          f"[QUARANTINE] Batch {batch_id} failed checks. Isolating {quarantined_count} bad records."
      )

      valid_df = processed_silver_df.filter(
          (col("amount") > 0)
          & col("amount").isNotNull()
          & col("transaction_id").isNotNull()
          & col("sender_account").isNotNull()
      )

      quarantine_df = processed_silver_df.filter(
          (col("amount") <= 0)
          | col("amount").isNull()
          | col("transaction_id").isNull()
          | col("sender_account").isNull()
      ).withColumn(
          "quarantine_reason",
          lit("Failed amount or null check validation"),
      )

      # Write clean data to main silver table
      valid_df.write.format("delta").mode("append").saveAsTable(
          "finance.silver.transactions_cleaned"
      )
      # Write bad data to quarantine table
      quarantine_df.write.format("delta").mode("append").saveAsTable(
          "finance.silver.transactions_quarantine"
      )

    elif behavior_mode == "WARNING_LOAD":
      print(
          f"[WARNING_LOAD] WARNING: Batch {batch_id} failed checks ({quarantined_count} bad records), but loading anyway per config."
      )
      processed_silver_df.write.format("delta").mode("append").saveAsTable(
          "finance.silver.transactions_cleaned"
      )

  else:
    # If status is PASS, normal write to the clean silver table
    processed_silver_df.write.format("delta").mode("append").saveAsTable(
        "finance.silver.transactions_cleaned"
    )

  # 6. Clean up memory cache
  batch_df.unpersist()

# COMMAND ----------

# 1. Read the stream from the Bronze table source
silver_input_stream = spark.readStream.table("finance.bronze.transactions_raw")

# 2. Execute the streaming query using writeStream and foreachBatch
silver_query = (
    silver_input_stream.writeStream.foreachBatch(process_silver_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/Volumes/finance/bronze/operational_data_vol/metadata/_checkpoints/silver_transactions_v2",
    )
    .trigger(availableNow=True)
    .start()
)
silver_query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finance.silver.transactions_quarantine