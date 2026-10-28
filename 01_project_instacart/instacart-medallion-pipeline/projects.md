@"
# Instacart Medallion Pipeline Architecture

## Overview
This project implements an end-to-end, production-ready **Medallion Architecture (Bronze → Silver → Gold)** on Azure Databricks using PySpark and Delta Lake. It processes raw Instacart e-commerce relational datasets (orders, products, aisles, departments, and order-products mapping) into optimized analytics and reporting layers.

The pipeline ensures data quality through schema enforcement, maintains data lineage with audit logging, and optimizes downstream querying performance using Delta Lake features like Liquid Clustering.

---

## Tech Stack & Core Technologies
- **Platform**: Azure Databricks (Databricks Runtime / Unity Catalog)
- **Compute Engine**: Apache Spark (PySpark)
- **Storage & Format**: Delta Lake (.parquet format with transaction log)
- **Orchestration**: Databricks Workflows / Jobs (YAML-driven configs)
- **Language & Tooling**: Python 3.x, SQL, Git version control

---

## Architecture & Data Flow

[ Raw CSV Datasets ]
        │
        ▼
   ┌─────────┐
   │ BRONZE  │  (Raw ingestion, metadata tracking, schema preservation)
   └────┬────┘
        │
        ▼
   ┌─────────┐
   │ SILVER  │  (Data cleaning, deduplication, type casting, auditing)
   └────┬────┘
        │
        ▼
   ┌─────────┐
   │  GOLD   │  (Business aggregations, dimensional modeling, KPI datasets)
   └─────────┘

### 1. Bronze Layer (Raw Ingestion)
* **Purpose**: Ingests raw source datasets as append-only Delta tables with zero data transformation.
* **Key Operations**:
  * Preserves raw data fidelity while appending ingested timestamps (`ingested_at`) and source file paths (`input_file_name`).
  * Enforces schema-on-read while allowing safe schema evolution where appropriate.

### 2. Silver Layer (Cleansing & Standardization)
* **Purpose**: Produces clean, standardized, and validated tables ready for ad-hoc queries and downstream modeling.
* **Key Operations**:
  * **Data Cleansing**: Null handling, string trimming, and explicit data type casting (e.g., timestamps, integer IDs).
  * **Deduplication**: Deduplicates transactional records using composite keys (e.g., `order_id` + `product_id`).
  * **Data Quality Checks**: Flags or filters invalid records (e.g., non-positive order sequence numbers or missing user references).
  * **Audit Logging**: Logs job execution metrics including record counts, processing runtimes, and status to an audit table.

### 3. Gold Layer (Business Aggregations & Analytics)
* **Purpose**: Delivers aggregated, business-ready tables modeled for fast analytical reporting, dashboards, and ML features.
* **Key Deliverables**:
  * **User Purchasing Profiles**: User-level reorder rates, order frequency, total lifetime orders, and peak ordering days/hours.
  * **Product Performance**: Top reordered products, aisle/department volume analysis, and basket affinity metrics.
  * **Table Optimization**: Organized using Liquid Clustering on high-cardinality keys to streamline analytical workloads and accelerate query execution.

---

## Project Repository Structure

instacart-medallion-pipeline/
├── conf/                     # Environment & job deployment configurations
│   └── pipeline_config.yaml  # Paths, database target schemas, cluster parameters
├── src/                      # Source code modules
│   ├── bronze/               # Bronze ingestion PySpark scripts
│   ├── silver/               # Silver cleansing & transformation PySpark scripts
│   ├── gold/                 # Gold analytical aggregation PySpark scripts
│   └── utils/                # Shared utilities (Spark session, audit logger, Delta helpers)
├── orchestration/            # Databricks Workflow job definitions / YAML specifications
└── data/                     # Sample/mock local schemas (actual datasets excluded via .gitignore)

---

## Key Technical Highlights
1. **ACID Transactions & Time Travel**: Built on Delta Lake to enable transaction isolation and time-travel capability for historic data auditing.
2. **Liquid Clustering for Performance**: Leverages Delta Lake Liquid Clustering to flexibly organize data layout, replacing rigid partitioning schemes and boosting analytical scan speeds.
3. **Automated Audit Logging**: Shared utility capturing execution metrics across all pipeline stages:
   * `pipeline_run_id`, `layer`, `table_name`, `records_processed`, `status`, and `timestamp`.
4. **Idempotent Pipeline Runs**: Uses dynamic partition overwrites or upsert logic (`MERGE INTO`) to guarantee clean re-runs without duplicate records.
5. **Modular & Scalable Design**: Separated pipeline concerns (ingestion vs. cleaning vs. aggregation) allowing independent scaling and execution per layer.

---

## Getting Started & Execution
* **Prerequisites**: Databricks Workspace (or local PySpark environment), Python 3.9+, Access to Instacart source files.
* **Setup**:
  ```bash
  git clone [https://github.com/karunadmhatre-sudo/karuna_databricks_projects.git](https://github.com/karunadmhatre-sudo/karuna_databricks_projects.git)
  cd karuna_databricks_projects/instacart-medallion-pipeline

