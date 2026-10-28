# Databricks notebook source
from pyspark.sql.functions import col, expr

# --- PARAMETERS (Can be passed via dbutils.widgets or Job Task Parameters) ---
# Example values: "order_products_prior" or "order_products_train"

dbutils.widgets.text("table_name", "order_products_prior")
target_table_name = dbutils.widgets.get("table_name").lower().strip()

# Define dynamic paths based on the parameter
bronze_table = f"instacart.bronze.{target_table_name}"
silver_table = f"instacart.silver.{target_table_name}"
checkpoint_location = f"/Volumes/instacart/bronze/operational_data_vol/metadata/_checkpoints/silver_{target_table_name}"

print(f"Processing Pipeline for: {bronze_table} -> {silver_table}")

# 1. Read raw bronze stream dynamically
bronze_df = spark.readStream.table(bronze_table)

# 2. Read silver products dimension for surrogate key lookup
silver_products_df = spark.read.table("instacart.silver.products")

# 3. Clean, safe-cast types, and enforce NOT NULL constraints
cleaned_df = (
    bronze_df
    .withColumn("order_id", expr("try_cast(order_id as long)"))
    .withColumn("product_id", expr("try_cast(product_id as long)"))
    .withColumn("add_to_cart_order", expr("try_cast(add_to_cart_order as int)"))
    .withColumn("reordered", expr("try_cast(reordered as int)"))
    .filter(col("order_id").isNotNull() & col("product_id").isNotNull())
)

# 4. Join with products dimension to attach the surrogate key (product_sk)
enriched_df = (
    cleaned_df
    .join(
        silver_products_df.select("product_id", "product_sk", "end_date"),
        on="product_id",
        how="inner"
    )
    # Ensure we only match active product dimension records (SCD Type 2 handling)
    .filter(col("end_date").isNull())
    .select(
        "order_id",
        "product_id",
        "product_sk",
        "add_to_cart_order",
        "reordered"
    )
    .dropDuplicates(["order_id", "product_id"])
)

# 5. Write to the target Silver Delta Table using AvailableNow trigger
query = (
    enriched_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_location)
    .toTable(silver_table)
)

query.awaitTermination()

print(f"Successfully completed processing stream for {silver_table}!")