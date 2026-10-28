# Databricks notebook source
from pyspark.sql.functions import col, count, avg, sum, expr

print("Starting Gold Layer: Building customer_features...")

# 1. Read the Gold fact_order_items table we just created
fact_df = spark.read.table("instacart.gold.fact_order_items")

# 2. Aggregate customer-level purchasing behaviors and features
customer_features_df = (
    fact_df
    .groupBy("user_id")
    .agg(
        # Order volume & frequency metrics
        count("order_id").alias("total_items_purchased"),
        expr("count(distinct order_id)").alias("total_orders"),
        avg("days_since_prior_order").alias("avg_days_between_orders"),
        
        # Product & reorder preferences
        sum("reordered").alias("total_reordered_items"),
        avg("reordered").alias("overall_reorder_rate"),
        
        # Timing preferences
        avg("order_dow").alias("avg_order_dow"),
        avg("order_hour_of_day").alias("avg_order_hour_of_day")
    )
    .withColumn(
        "reorder_intensity_ratio", 
        col("total_reordered_items") / col("total_items_purchased")
    )
)

# 3. Ensure the gold schema exists in Unity Catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS instacart.gold")

# 4. Write out to the Gold Customer Feature Mart using batch overwrite
(
    customer_features_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("instacart.gold.customer_features")
)

print("Successfully created instacart.gold.customer_features data mart!")