# Databricks notebook source
# MAGIC %sql
# MAGIC spark.sql("""CREATE TABLE IF NOT EXISTS instacart.silver.pipeline_audit_log (
# MAGIC     table_name STRING COMMENT 'Name of the target silver table processed',
# MAGIC     commit_timestamp TIMESTAMP COMMENT 'Exact timestamp of the Delta commit',
# MAGIC     rows_inserted BIGINT COMMENT 'Number of new records inserted/versioned',
# MAGIC     rows_updated BIGINT COMMENT 'Number of records updated/expired',
# MAGIC     rows_deleted BIGINT COMMENT 'Number of records deleted',
# MAGIC     execution_time TIMESTAMP COMMENT 'Time when the audit log entry was recorded'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Centralized audit log capturing execution metrics for Silver layer pipelines'
# MAGIC """)

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

dimension_tables = ["products", "aisles", "departments","orders","order_products_prior","order_products_train"]

for dim in dimension_tables:
    table_name = f"instacart.silver.{dim}"
    
    try:
        history_df = spark.sql(f"""
            SELECT timestamp, operationMetrics 
            FROM (DESCRIBE HISTORY {table_name}) 
            ORDER BY version DESC 
            LIMIT 1
        """)
        
        row = history_df.collect()[0]
        # Format timestamp to string so SQL handles it safely
        commit_timestamp = str(row["timestamp"]) if row["timestamp"] else None
        metrics = row["operationMetrics"] or {}

        num_inserted = int(metrics.get("numTargetRowsInserted", 0))
        num_updated = int(metrics.get("numTargetRowsUpdated", 0))
        num_deleted = int(metrics.get("numTargetRowsDeleted", 0))
        
    except Exception as e:
        commit_timestamp = None
        num_inserted, num_updated, num_deleted = 0, 0, 0
        print(f"Could not fetch history for {table_name}: {e}")

    # Ensure commit_timestamp is handled safely for SQL text insertion
    timestamp_val = f"'{commit_timestamp}'" if commit_timestamp else "NULL"

    # Execute the insert matching the exact column order of your table:
    # (table_name, commit_timestamp, rows_inserted, rows_updated, rows_deleted, execution_time)
    spark.sql(f"""
        INSERT INTO instacart.silver.pipeline_audit_log 
        VALUES (
            '{table_name}', 
            {timestamp_val}, 
            {num_inserted}, 
            {num_updated}, 
            {num_deleted}, 
            current_timestamp()
        )
    """)