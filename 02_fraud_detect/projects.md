# Enterprise Delta Lake Architecture: Fraud Detection Pipeline

## Architecture Overview
This project implements an end-to-end, enterprise-grade **Medallion (Bronze-Silver-Gold) Data Lakehouse** architecture on Databricks using Delta Lake, Structured Streaming, and Liquid Clustering. The system processes high-volume financial transaction streams, enforces strict data quality validations, routes quarantine data, and produces near-real-time security alerts and daily merchant risk analytics.

---

## Pipeline Layers & Implementation Details

### Bronze Layer (`finance.bronze.transactions_raw`)
* **Role**: Raw ingestion layer preserving source payload metadata and handling malformed data.
* **Key Features**: 
  * Ingests incoming batch streaming files with append-only metadata tracking (`source_file_path`, `ingestion_time`, `batch_id`).
  * **Rescued Data Tracking**: Captures schema drift or malformed records using Auto Loader's rescued data mechanism (`_rescued_data`) to prevent ingestion failures and maintain full data fidelity.

### Silver Layer (`finance.silver.transactions_cleaned`)
* **Role**: Data cleaning, deduplication, watermarking, dynamic risk scoring, and quality control.
* **Liquid Clustering**: `CLUSTER BY (sender_account, transaction_type)`
* **Dynamic Behavior Modes**:
  Controlled dynamically via job parameter or Databricks Widget (`behavior_mode`), taking one of three values: `HARD_STOP`, `QUARANTINE`, or `WARNING_LOAD`.
  * **`HARD_STOP`**: Aborts the streaming query immediately upon encountering data quality check failures.
  * **`QUARANTINE`**: Isolates failing records into `finance.silver.transactions_quarantine` (built using `CREATE TABLE ... LIKE finance.silver.transactions_cleaned` with an added `quarantine_reason` column) while allowing valid records to proceed into the clean Silver table.
  * **`WARNING_LOAD`**: Logs audit violations to `finance.silver.quality_metrics` while writing all records to the Silver table without interrupting execution.
* **Metrics Logging**: Captures `total_records`, `invalid_amount_count`, `null_id_count`, `quarantined_count`, `quality_status`, and `execution_mode` into `finance.silver.quality_metrics`.

### Gold Layer
#### 1. High-Risk Security Alerts Stream (`finance.gold.high_risk_alerts`)
- **Role**: Operational real-time security alert pipeline.
- **Liquid Clustering**: `CLUSTER BY (initial_risk_tier, timestamp)`
- **Inclusion Criteria**: Captures transactions flagged with `initial_risk_tier = 'HIGH'`, `velocity_score > 80`, or `geo_anomaly_score > 80`.
- **Lineage Tracking**: Preserves `batch_id` to maintain explicit row-level traceability back to Silver and Bronze batches.

#### 2. Daily Merchant Analytics (`finance.gold.merchant_risk_daily`)
- **Role**: Micro-batch/batch aggregated analytics model for BI dashboards.
- **Liquid Clustering**: `CLUSTER BY (processing_date, merchant_category)`
- **Metrics Tracked**: `total_transactions`, `total_amount`, `avg_transaction_amount`, `high_risk_transaction_count`, `avg_velocity_score`, and `avg_geo_anomaly_score`.

#### 3. Rapid Purchase Velocity Stream (`finance.gold.rapid_purchase_alerts`)
- **Role**: Behavioral fraud detection pipeline tracking rapid repeat transactions and spending velocity.
- **Core Mechanics**: Implements stateful stream-to-stream self-joins with a 5-minute event-relative rolling lookback window and 10-minute watermarking.
- **Inclusion Criteria**: Triggers alerts when a sender's transactions cross thresholds (`rolling_tx_count >= 3` or `rolling_total_amount > 500.0`) within the lookback window.
- **Execution & Sink Pattern**: Uses `outputMode("update")` paired with `foreachBatch` and Delta `.merge()` upserts to handle stateful record mutations cleanly.
---

### Pipeline Data Lineage & Flow
- **End-to-End Traceability**: Lineage is explicitly maintained across all medallion layers

