# Databricks notebook source
from pyspark.sql.functions import col, expr, current_timestamp

# 1. Read raw bronze orders stream or batch
checkpoint_location = f"/Volumes/instacart/bronze/operational_data_vol/metadata/_checkpoints/silver_orders"
bronze_orders_df = spark.readStream.table("instacart.bronze.orders")

# 2. Clean, safe-cast types, and explicitly enforce NOT NULL on order_id and user_id
silver_orders_df = (
    bronze_orders_df
    .withColumn("order_id", expr("try_cast(order_id as long)"))
    .withColumn("user_id", expr("try_cast(user_id as long)"))
    .withColumn("order_number", expr("try_cast(order_number as int)"))
    .withColumn("order_dow", expr("try_cast(order_dow as int)"))
    .withColumn("order_hour_of_day", expr("try_cast(order_hour_of_day as int)"))
    .withColumn("days_since_prior_order", expr("try_cast(days_since_prior_order as float)"))
    .filter(col("order_id").isNotNull() & col("user_id").isNotNull())
    .dropDuplicates(["order_id"])
)

# 3. Write to Silver Orders Delta Table
#dbutils.fs.rm("/Volumes/instacart/bronze/operational_data_vol/metadata/_checkpoints/orders", recurse=True)
#print("Checkpoint cleared successfully!")
query = (
    silver_orders_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_location)
    .toTable("instacart.silver.orders")
)
query.awaitTermination()