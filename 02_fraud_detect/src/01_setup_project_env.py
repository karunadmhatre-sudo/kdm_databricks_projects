# Databricks notebook source
# MAGIC %md
# MAGIC ## Set-up the project environment for GizmoBox Data Lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Access the container Telemtry

# COMMAND ----------

# 1. Drop tables to clear out old states/schemas
spark.sql("DROP TABLE IF EXISTS finance.bronze.transactions_raw")
spark.sql("DROP TABLE IF EXISTS finance.silver.transactions_cleaned")
spark.sql("DROP TABLE IF EXISTS finance.silver.transactions_quarantine")
spark.sql("DROP TABLE IF EXISTS finance.bronze.data_load_stats")
spark.sql("DROP TABLE IF EXISTS finance.silver.quality_metrics")

# 2. Wipe the entire checkpoint directory cleanly
dbutils.fs.rm("/Volumes/finance/bronze/operational_data_vol/metadata/_checkpoints/", recurse=True)

print("Successfully wiped all tables and checkpoint metadata!")

# COMMAND ----------

dbutils.fs.rm("/Volumes/finance/bronze/operational_data_vol/metadata/_checkpoints/gold_high_risk_alerts_v1", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils.fs.ls(
# MAGIC     "/Volumes/finance/bronze/operational_data_vol/metadata/_checkpoints/"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create External Location

# COMMAND ----------


spark.sql("""CREATE EXTERNAL LOCATION IF NOT EXISTS finance_ext_dl
    URL 'abfss://fraud@karunaextdl.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL karuna_db_credentials)
    COMMENT 'External Location For the instacart Data Lakehouse'
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create the catalog - instacart

# COMMAND ----------

spark.sql("""CREATE CATALOG IF NOT EXISTS finance
      MANAGED LOCATION 'abfss://fraud@karunaextdl.dfs.core.windows.net/'
      COMMENT 'This is the catalog for the Finance Data Lakehouse' """) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Create Schemas
# MAGIC 1. Landing
# MAGIC 1. Bronze
# MAGIC 1. Silver
# MAGIC 1. Gold

# COMMAND ----------

# Switch catalog
spark.sql("USE CATALOG finance")

# Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS finance.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS finance.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS finance.gold")      

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Create Volume

# COMMAND ----------

spark.sql("USE CATALOG finance")
spark.sql("USE SCHEMA bronze")

spark.sql("DROP VOLUME IF EXISTS finance.bronze.operational_data_vol")

spark.sql("""
    CREATE EXTERNAL VOLUME finance.bronze.operational_data_vol
    COMMENT 'External volume pointing to raw landing operational data CSVs'
    LOCATION 'abfss://fraud@karunaextdl.dfs.core.windows.net/landing/operational_data/'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Test loading just the orders file using the abfss path directly
# MAGIC test_path = "abfss://fraud@karunaextdl.dfs.core.windows.net/landing/operational_data/finance/financial_fraud_detection_dataset.csv"
# MAGIC
# MAGIC df_test = spark.read.format("csv").option("header", "true").load(test_path)
# MAGIC display(df_test.limit(5))

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS finance.bronze.data_load_stats (
    run_timestamp TIMESTAMP,
    total_rows LONG,
    rescued_rows LONG,
    clean_rows LONG,
    rescued_percentage DOUBLE,
    status STRING
)
USING DELTA""")

spark.sql("""DROP TABLE finance.silver.transactions_cleaned""")

spark.sql("""CREATE TABLE IF NOT EXISTS finance.silver.transactions_cleaned (
    transaction_id STRING,
    timestamp TIMESTAMP,
    sender_account STRING,
    receiver_account STRING,
    amount DOUBLE,
    transaction_type STRING,
    merchant_category STRING,
    location STRING,
    device_used STRING,
    is_fraud BOOLEAN,
    fraud_type STRING,
    time_since_last_transaction DOUBLE,
    spending_deviation_score DOUBLE,
    velocity_score DOUBLE,
    geo_anomaly_score DOUBLE,
    payment_channel STRING,
    ip_address STRING,
    device_hash STRING,
    processed_at TIMESTAMP,
    initial_risk_tier STRING,
    source_file_path STRING,
    ingestion_time TIMESTAMP,
    batch_id INT
)
USING DELTA
CLUSTER BY (sender_account, transaction_type)""")

spark.sql("""CREATE TABLE IF NOT EXISTS finance.silver.transactions_quarantine 
LIKE finance.silver.transactions_cleaned""")


spark.sql("""ALTER TABLE finance.silver.transactions_quarantine ADD COLUMNS (
    quarantine_reason STRING
)""")



# COMMAND ----------

# MAGIC %sql
# MAGIC spark.sql("""drop table finance.gold.high_risk_alerts""")
# MAGIC
# MAGIC spark.sql("""CREATE TABLE IF NOT EXISTS finance.gold.high_risk_alerts (
# MAGIC     transaction_id STRING,
# MAGIC     timestamp TIMESTAMP,
# MAGIC     sender_account STRING,
# MAGIC     receiver_account STRING,
# MAGIC     amount DOUBLE,
# MAGIC     transaction_type STRING,
# MAGIC     merchant_category STRING,
# MAGIC     payment_channel STRING,
# MAGIC     velocity_score DOUBLE,
# MAGIC     geo_anomaly_score DOUBLE,
# MAGIC     initial_risk_tier STRING,
# MAGIC     processed_at TIMESTAMP,
# MAGIC     batch_id INT, -- Added for line-of-sight tracking back to Silver/Bronze
# MAGIC     alert_generated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (initial_risk_tier, timestamp)""")
# MAGIC
# MAGIC
# MAGIC spark.sql("""CREATE TABLE IF NOT EXISTS finance.gold.merchant_risk_daily (
# MAGIC     processing_date DATE,
# MAGIC     merchant_category STRING,
# MAGIC     payment_channel STRING,
# MAGIC     total_transactions BIGINT,
# MAGIC     total_amount DOUBLE,
# MAGIC     avg_transaction_amount DOUBLE,
# MAGIC     high_risk_transaction_count BIGINT,
# MAGIC     avg_velocity_score DOUBLE,
# MAGIC     avg_geo_anomaly_score DOUBLE,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (processing_date, merchant_category)""")
# MAGIC
# MAGIC --