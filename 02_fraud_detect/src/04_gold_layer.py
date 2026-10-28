# Databricks notebook source
# ==========================================
# GOLD LAYER: Pipeline Execution
# ==========================================
from pyspark.sql.functions import avg, col, count, current_timestamp, sum, when

# ------------------------------------------
# 1. Pipeline A: High-Risk Alerts Stream
# ------------------------------------------


def process_gold_alerts_batch(batch_df, batch_id):
  # Filter for high-risk records and project Gold alert schema (including batch_id lineage)
  alerts_df = (
      batch_df.filter(
          (col("initial_risk_tier") == "HIGH")
          | (col("velocity_score") > 80)
          | (col("geo_anomaly_score") > 80)
      )
      .withColumn("alert_generated_at", current_timestamp())
      .select(
          "transaction_id",
          "timestamp",
          "sender_account",
          "receiver_account",
          "amount",
          "transaction_type",
          "merchant_category",
          "payment_channel",
          "velocity_score",
          "geo_anomaly_score",
          "initial_risk_tier",
          "processed_at",
          "batch_id",  # <--- Preserves exact lineage match with Silver
          "alert_generated_at",
      )
  )
  alerts_df.write.format("delta").mode("append").saveAsTable(
      "finance.gold.high_risk_alerts"
  )


# Stream from Silver to Gold Alerts
silver_stream = spark.readStream.table("finance.silver.transactions_cleaned")

gold_alerts_query = (
    silver_stream.writeStream.foreachBatch(process_gold_alerts_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/Volumes/finance/bronze/operational_data_vol/metadata/_checkpoints/gold_high_risk_alerts_v1",
    )
    .trigger(availableNow=True)
    .start()
)

# Wait for alerts stream completion
gold_alerts_query.awaitTermination()

# ------------------------------------------
# 2. Pipeline B: Daily Merchant Risk Summary (Batch Aggregation)
# ------------------------------------------
# Compute daily metrics aggregated from clean Silver data
merchant_summary_df = (
    spark.table("finance.silver.transactions_cleaned")
    .withColumn("processing_date", col("timestamp").cast("date"))
    .groupBy("processing_date", "merchant_category", "payment_channel")
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_transaction_amount"),
        sum(when(col("initial_risk_tier") == "HIGH", 1).otherwise(0)).alias(
            "high_risk_transaction_count"
        ),
        avg("velocity_score").alias("avg_velocity_score"),
        avg("geo_anomaly_score").alias("avg_geo_anomaly_score"),
    )
    .withColumn("updated_at", current_timestamp())
)

# Overwrite/Merge daily metrics into Gold Summary table
merchant_summary_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("finance.gold.merchant_risk_daily")

print("🚀 Gold Layer processing complete: Alerts stream and Daily Merchant aggregations updated successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Check Bronze raw input count
# MAGIC SELECT 'Bronze Raw' AS layer, count(*) AS total_rows FROM finance.bronze.transactions_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 2. Check Silver clean count
# MAGIC SELECT 'Silver Cleaned' AS layer, count(*) AS total_rows FROM finance.silver.transactions_cleaned
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 3. Check Gold High-Risk Alerts count
# MAGIC SELECT 'Gold Alerts' AS layer, count(*) AS total_rows FROM finance.gold.high_risk_alerts
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- 4. Check Gold Daily Summary count
# MAGIC SELECT 'Gold Merchant Summary' AS layer, count(*) AS total_rows FROM finance.gold.merchant_risk_daily;