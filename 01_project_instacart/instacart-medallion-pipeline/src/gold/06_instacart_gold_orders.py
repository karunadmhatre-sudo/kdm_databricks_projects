# Databricks notebook source
from pyspark.sql.functions import col, lit

print("Starting Gold Layer: Building fact_order_items...")

# 1. Read Silver tables
orders_df = spark.read.table("instacart.silver.orders")
products_df = spark.read.table("instacart.silver.products")
prior_df = spark.read.table("instacart.silver.order_products_prior")
train_df = spark.read.table("instacart.silver.order_products_train")

# 2. Combine prior and train line items into a single unified analytical fact stream 
union_items_df = (
    prior_df.withColumn("data_split", lit("prior"))
    .unionByName(train_df.withColumn("data_split", lit("train")))
)

# 3. Build the Gold Fact Order Items table using explicit aliases and qualified columns
gold_fact_order_items = (
    union_items_df.alias("items")
    .join(
        orders_df.alias("ords"), 
        on="order_id", 
        how="inner"
    )
    .join(
        products_df.filter(col("end_date").isNull()).alias("prods"),
        on="product_sk",
        how="inner"
    )
    .select(
        col("items.order_id"),
        col("ords.user_id"),
        col("items.product_id"),
        col("items.product_sk"),
        col("prods.product_name"),
        col("prods.aisle_id"),
        col("prods.department_id"),
        col("items.add_to_cart_order"),
        col("items.reordered"),
        col("ords.order_number"),
        col("ords.order_dow"),
        col("ords.order_hour_of_day"),
        col("ords.days_since_prior_order"),
        col("items.data_split")
    )
)

# 4. Ensure the gold schema exists in Unity Catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS instacart.gold")

# 5. Write out to the Gold layer as a managed Delta table using batch overwrite
(
    gold_fact_order_items.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("instacart.gold.fact_order_items")
)

print("Successfully created instacart.gold.fact_order_items data mart!")