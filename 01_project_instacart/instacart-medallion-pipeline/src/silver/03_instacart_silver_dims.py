# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, trim, current_timestamp, current_date, md5, concat_ws, lit, expr

# Create a widget for the dimension name (defaults to 'all' for manual testing)
dbutils.widgets.text("dim_name", "all")
dim_name = dbutils.widgets.get("dim_name").lower().strip()

print("==================================================")
print(f"STARTING SILVER DIMENSION PROCESSING FOR: {dim_name}")
print("==================================================")


# ==========================================================
# 1. PROCESS AISLES (SCD Type 1 - Overwrite)
# ==========================================================
if dim_name in ["all", "aisles"]:
    print("\n--- Processing Silver: Aisles (SCD Type 1) ---")
    df_aisles = (
        spark.read.table("instacart.bronze.aisles")
        .withColumn("aisle_id", col("aisle_id").cast("integer"))
        .withColumn("aisle", trim(col("aisle")))
        .dropDuplicates(["aisle_id"])
        .withColumn("processed_timestamp", current_timestamp())
    )

    (
        df_aisles.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("instacart.silver.aisles")
    )
    print("Successfully updated: instacart.silver.aisles")


# ==========================================================
# 2. PROCESS DEPARTMENTS (SCD Type 1 - Overwrite)
# ==========================================================
if dim_name in ["all", "departments"]:
    print("\n--- Processing Silver: Departments (SCD Type 1) ---")
    df_departments = (
        spark.read.table("instacart.bronze.departments")
        .withColumn("department_id", col("department_id").cast("integer"))
        .withColumn("department", trim(col("department")))
        .dropDuplicates(["department_id"])
        .withColumn("processed_timestamp", current_timestamp())
    )

    (
        df_departments.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("instacart.silver.departments")
    )
    print("Successfully updated: instacart.silver.departments")


# ==========================================================
# 3. PROCESS PRODUCTS (SCD Type 2 - Delta MERGE)
# ==========================================================
if dim_name in ["all", "products"]:
    print("\n--- Processing Silver: Products (SCD Type 2) ---")
    target_table_name = "instacart.silver.products"

    # 1. Read Bronze and isolate ONLY the latest snapshot using explicit ingestion timestamp
    print("Reading and isolating latest snapshot from Bronze products...")
    window_spec = Window.partitionBy("product_id").orderBy(col("ingestion_timestamp").desc())

    df_bronze_products = (
        spark.read.table("instacart.bronze.products")
        .withColumn("product_id", expr("try_cast(product_id as integer)"))
        .withColumn("aisle_id", expr("try_cast(aisle_id as integer)"))
        .withColumn("department_id", expr("try_cast(department_id as integer)"))
        .withColumn("product_name", trim(col("product_name")))
        .filter(col("product_id").isNotNull()) 
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .dropDuplicates(["product_id"])
    )
    
    # 2. First-time table initialization if it doesn't exist yet
    if not spark.catalog.tableExists(target_table_name):
        print(f"Table {target_table_name} does not exist. Initializing as SCD Type 2...")

        (
            df_bronze_products
            .withColumn("product_sk", md5(concat_ws("-", col("product_id"), current_timestamp())))
            .withColumn("start_date", current_date())
            .withColumn("end_date", lit(None).cast("date"))
            .withColumn("is_current", lit(True))
            .withColumn("processed_timestamp", current_timestamp())
            .write.format("delta")
            .mode("overwrite")
            .saveAsTable(target_table_name)
        )
        print(f"Successfully initialized and loaded {target_table_name}!")
    else:
        print(f"Executing SCD Type 2 Single-Merge Processing for {target_table_name}...")
        target_table = DeltaTable.forName(spark, target_table_name)
        df_active_target = target_table.toDF().filter(col("is_current") == True)

        # 1. Base join and flags
        df_base = (
            df_bronze_products.alias("src")
            .join(df_active_target.alias("tgt"), "product_id", "left")
            .withColumn("product_id_join", lit(-999999)) 
            .withColumn("exists_in_target", col("tgt.product_id").isNotNull())
            .withColumn(
                "is_changed",
                col("exists_in_target") & (
                    (col("src.aisle_id") != col("tgt.aisle_id")) | 
                    (col("src.department_id") != col("tgt.department_id"))
                )
            )
            .filter(~col("exists_in_target") | col("is_changed"))
            .select("src.*", "is_changed", "exists_in_target", "product_id_join")
        )

        # 2. For changed records, we need a "closer" row that uses the real product_id to match and close history
        df_to_close = (
            df_base
            .filter(col("exists_in_target") & col("is_changed"))
            .withColumn("product_id_join", col("product_id"))
        )

        # 3. Union df_to_close back into df_base so df_staged has both the closer and the -999999 inserter rows
        df_staged = df_base.unionByName(df_to_close)

        # 4. SINGLE MERGE EXECUTION
        print("Executing single-statement SCD Type 2 MERGE...")

        (
            target_table.alias("tgt")
            .merge(
                df_staged.alias("src"),
                "tgt.product_id = src.product_id_join AND tgt.is_current = true"
            )
            .whenMatchedUpdate(
                set={
                    "is_current": "false",
                    "end_date": "current_date()"
                }
            )
            .whenNotMatchedInsert(
                values={
                    "product_sk": "md5(concat_ws('-', src.product_id, current_timestamp()))",
                    "product_id": "src.product_id",
                    "product_name": "src.product_name",
                    "aisle_id": "src.aisle_id",
                    "department_id": "src.department_id",
                    "start_date": "current_date()",
                    "end_date": "null",
                    "is_current": "true",
                    "processed_timestamp": "current_timestamp()"
                }
            )
            .execute()
        )

    print(f"Successfully completed single-merge SCD Type 2 pipeline for {target_table_name}!")