# Databricks notebook source
# Create a widget for the table name (defaults to 'orders' for manual testing)
dbutils.widgets.text("table_name", "orders")
table_name = dbutils.widgets.get("table_name")

print(f"Starting Bronze ingestion for: {table_name}")

from pyspark.sql.functions import current_timestamp, col

# Dynamic paths based on the parameter
source_path = f"/Volumes/instacart/bronze/operational_data_vol/{table_name}/"
schema_location = f"/Volumes/instacart/bronze/operational_data_vol/metadata/_schemas/{table_name}"
checkpoint_location = f"/Volumes/instacart/bronze/operational_data_vol/metadata/_checkpoints/{table_name}"
target_table = f"instacart.bronze.{table_name}"

# Autoloader stream
df_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", schema_location)
    .load(source_path)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)

# Write to Delta
query = (
    df_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_location)
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()
print(f"Successfully finished ingestion for: {target_table}")