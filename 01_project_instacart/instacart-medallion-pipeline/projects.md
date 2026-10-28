@"
# Instacart Medallion Pipeline Architecture

## Overview
This project implements an end-to-end, production-ready **Medallion Architecture (Bronze → Silver → Gold)** on Azure Databricks using PySpark and Delta Lake. It processes raw Instacart e-commerce relational datasets (orders, products, aisles, departments, and order-products mapping) into optimized analytics and reporting layers.

The pipeline ensures data quality through schema enforcement, maintains data lineage with audit logging, and optimizes downstream querying performance using Delta Lake features like Liquid Clustering.

### 1. Bronze Layer (Raw Ingestion)
- **Purpose**: Ingests raw source datasets as append-only Delta tables with zero data transformation.
- **Key Operations**:
  * Preserves raw data fidelity while appending ingested timestamps (`ingested_at`) and source file paths (`input_file_name`).
  * Enforces schema-on-read while allowing safe schema evolution where appropriate.
  * Utilizes a generic bronze ingestion pipeline powered by **Databricks Auto Loader** (`cloudFiles`) running in **append mode** to automatically ingest incoming files into their respective bronze tables for `aisles`, `departments`, `products`, `orders`, `order_products_prior`, and `order_products_train`.

### 2. Silver Layer (Cleansing & Standardization)
- **Purpose**: Produces clean, standardized, and validated tables ready for ad-hoc queries and downstream modeling.
- **Key Operations**:
  * **Data Cleansing**: Null handling, string trimming, and explicit data type casting (e.g., timestamps, integer IDs).
  * **Deduplication**: Deduplicates transactional records using composite keys (e.g., `order_id` + `product_id`).
  * **Data Quality Checks**: Flags or filters invalid records (e.g., non-positive order sequence numbers or missing user references).
  * **Slowly Changing Dimensions (SCD)**: Implemented SCD Type 1 for `aisles` and `departments`, and SCD Type 2 for `products` utilizing a surrogate key (`product_sk`) to track historical versions of product data.
  * **Incremental Fact Loading**: Fact tables were incrementally loaded by joining bronze orders data with dimension data.
  * **Audit Logging**: Tracks execution metrics, tracking row-level mutations and processing performance for each silver table build.

### 3. Gold Layer (Business Aggregations & Analytics)
* **Purpose**: Delivers aggregated, business-ready tables modeled for fast analytical reporting, dashboards, and ML features.
* **Key Deliverables**:
  * **User Purchasing Profiles**: User-level reorder rates, order frequency, total lifetime orders, and peak ordering days/hours.
  * **Product Performance**: Top reordered products, aisle/department volume analysis, and basket affinity metrics.
  * **Table Optimization**: Organized using Liquid Clustering on high-cardinality keys to streamline analytical workloads and accelerate query execution.

