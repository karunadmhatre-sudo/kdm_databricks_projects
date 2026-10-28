# Databricks notebook source
from pyspark.sql.functions import col, count, sum, avg, expr

print("Building Gold Product Performance (Standard Delta Table)...")

# 1. Read the Gold fact table
fact_df = spark.read.table("instacart.gold.fact_order_items")

# 2. Aggregate product-level metrics
product_performance_df = (
    fact_df
    .groupBy("product_id", "product_name", "aisle_id", "department_id")
    .agg(
        count("order_id").alias("total_times_ordered"),
        expr("count(distinct user_id)").alias("unique_customers_count"),
        sum("reordered").alias("total_reorders"),
        avg("reordered").alias("product_reorder_rate"),
        avg("add_to_cart_order").alias("avg_add_to_cart_position")
    )
)

# 3. Ensure schema exists and write out as a standard managed Delta table
spark.sql("CREATE SCHEMA IF NOT EXISTS instacart.gold")

(
    product_performance_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("instacart.gold.product_performance_python")
)

print("Successfully created standard Delta table: instacart.gold.product_performance")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from instacart.gold.product_performance_python