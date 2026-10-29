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

# ------------------------------------------
# 3. Pipeline C:Perform real-time aggregations (e.g., flagging transactions where amount >$5,000
#     or counting rapid repeat purchases within a 5-minute sliding window).
# ------------------------------------------


print("🚀 Gold Layer processing complete: Alerts stream and Daily Merchant aggregations updated successfully.")

# COMMAND ----------

# ==========================================
# GOLD LAYER: Perform real-time aggregations (e.g., flagging transactions where amount >$5,000or counting 
#  rapid repeat purchases within a 5-minute sliding window).
# ==========================================

from delta.tables import DeltaTable
from pyspark.sql.functions import col, count, current_timestamp, expr, lit, sum

# 1. Read from the Silver Layer Cleaned Transactions Stream
silver_stream = spark.readStream.table("finance.silver.transactions_cleaned")

# 2. Alias two instances of the stream for the rolling lookback self-join
stream_1 = silver_stream.withWatermark("timestamp", "10 minutes").alias("current_tx")
stream_2 = silver_stream.withWatermark("timestamp", "10 minutes").alias(
    "historical_tx"
)

# 3. Perform the stream-to-stream join with time constraints (5-minute rolling lookback)
rolling_velocity_stream = (
    stream_1.join(
        stream_2,
        (col("current_tx.sender_account") == col("historical_tx.sender_account"))
        & (
            col("historical_tx.timestamp")
            >= col("current_tx.timestamp") - expr("INTERVAL 5 MINUTES")
        )
        & (col("historical_tx.timestamp") <= col("current_tx.timestamp")),
        how="inner",
    )
    .groupBy(
        col("current_tx.transaction_id"),
        col("current_tx.sender_account"),
        col("current_tx.timestamp"),
        col("current_tx.amount"),
    )
    .agg(
        count("historical_tx.transaction_id").alias("rolling_tx_count"),
        sum("historical_tx.amount").alias("rolling_total_amount"),
    )
    # Filter for rapid purchase fraud thresholds
    .filter(
        (col("rolling_tx_count") >= 3)
        | (col("rolling_total_amount") > 500.0)
    )
    .withColumn("alert_type", lit("RAPID_PURCHASE_ROLLING_LOOKBACK"))
    .withColumn("alert_generated_at", current_timestamp())
)


# 4. Define foreachBatch function using Delta .merge() to safely upsert update-mode results
def upsert_rolling_alerts(batch_df, batch_id):
  # Ensure the target table exists before attempting a merge
  batch_df.sparkSession.sql("""
        CREATE TABLE IF NOT EXISTS finance.gold.rapid_purchase_alerts (
            transaction_id STRING,
            sender_account STRING,
            timestamp TIMESTAMP,
            amount DOUBLE,
            rolling_tx_count LONG,
            rolling_total_amount DOUBLE,
            alert_type STRING,
            alert_generated_at TIMESTAMP
        ) USING DELTA
    """)

  deltaTable = DeltaTable.forName(
      batch_df.sparkSession, "finance.gold.rapid_purchase_alerts"
  )

  (
      deltaTable.alias("target")
      .merge(
          batch_df.alias("source"),
          "target.transaction_id = source.transaction_id",
      )
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute()
  )


# 5. Start the Streaming Query using update mode with foreachBatch
rolling_query = (
    rolling_velocity_stream.writeStream.foreachBatch(upsert_rolling_alerts)
    .outputMode("update")  # Required for stateful stream-stream joins
    .option(
        "checkpointLocation",
        "/Volumes/finance/bronze/operational_data_vol/metadata/_checkpoints/gold_rolling_velocity_v1",
    )
    .trigger(availableNow=True)
    .start()
)

rolling_query.awaitTermination()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finance.gold.rapid_purchase_alerts