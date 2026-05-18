# Telecom Churn Data Warehouse

## Project Overview

End-to-end Telecom Churn Data Warehouse platform built using Apache Spark, Snowflake, dbt, and Apache Airflow following modern Data Engineering and Analytics Engineering practices.

This platform implements a production-style telecom analytics architecture capable of ingesting, transforming, validating, and modeling telecom operational data into business-ready analytics marts for churn analysis and reporting.

The project simulates real-world telecom customer activity data including:
- Customer profiles
- Call usage records
- Recharge transactions
- Customer complaints

The platform is designed using layered warehouse architecture, distributed processing, incremental pipelines, and modular transformations.

---

# Business Problem

Telecom companies generate massive volumes of customer interaction and operational data across multiple systems.

Business teams require centralized analytics platforms capable of:
- Monitoring customer activity
- Detecting churn signals
- Tracking complaints and service quality
- Analyzing recharge behavior
- Generating business intelligence dashboards

This project simulates a production-grade telecom analytics platform that processes raw telecom activity data and transforms it into analytics-ready marts for downstream reporting and churn analysis.

---

# Architecture

```text
Raw CSV Data
        ↓
Apache Spark Incremental Ingestion
        ↓
Processed Parquet Layer
        ↓
Snowflake STAGING Layer
        ↓
dbt STAGING Models
        ↓
dbt CORE Warehouse Models
        ↓
dbt MART Analytics Layer
        ↓
Dashboard & Business Analytics
```

---

# Tech Stack

| Category | Technology |
|---|---|
| Distributed Processing | Apache Spark, PySpark |
| Cloud Data Warehouse | Snowflake |
| Data Transformations | dbt |
| Orchestration | Apache Airflow |
| Programming Languages | Python, SQL |
| Storage Format | Parquet |
| Synthetic Data Generation | Faker |
| Version Control | Git, GitHub |
| Data Modeling | Kimball Star Schema |
| Analytics Engineering | dbt |
| Workflow Scheduling | Apache Airflow |

---

# Key Engineering Highlights

- Fully orchestrated end-to-end data pipeline using Apache Airflow
- Incremental PySpark ingestion framework with watermark tracking
- Layered Snowflake warehouse architecture (STAGING → CORE → MART)
- Modular dbt transformations with lineage tracking
- Incremental warehouse models using dbt incremental materializations
- Automated data quality validation using dbt tests
- Deduplication and standardized transformation logic
- Parquet-based processed data layer
- TaskGroup-based Airflow orchestration
- Production-style modular project structure

---

# Project Structure

```text
telecom-churn-dwh/
│
├── airflow/
│
├── architecture/
│
├── dashboards/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── data_generation/
│
├── metadata/
│
├── telecom_dbt/
│
├── spark_jobs/
│   ├── jobs/
│   └── utils/
│
├── configs/
│
├── screenshots/
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

# End-to-End Pipeline Flow

```text
customers.csv
calls.csv
complaints.csv
recharges.csv
        ↓
PySpark Incremental Ingestion
        ↓
Data Cleaning & Standardization
        ↓
Processed Parquet Layer
        ↓
Snowflake STAGING Tables
        ↓
dbt STAGING Models
        ↓
dbt CORE Warehouse Models
        ↓
dbt MART Analytics Models
        ↓
Business Dashboards & Analytics
```

---

# Data Generation Layer

Synthetic telecom datasets are generated using Faker to simulate real-world telecom operations.

## Generated Datasets

- customers.csv
- calls.csv
- complaints.csv
- recharges.csv

## Features

- Randomized customer profiles
- Telecom usage simulation
- Complaint event simulation
- Recharge transaction simulation
- Scalable synthetic data generation

---

# Spark Ingestion Framework

The Spark ingestion layer processes raw telecom datasets into standardized parquet datasets.

## Spark Features

- PySpark ingestion jobs
- Incremental ingestion framework
- Watermark-based processing
- Duplicate removal
- Data validation
- Schema standardization
- Modular Spark session utility
- Structured logging framework

## Spark Jobs

- ingest_customers.py
- ingest_calls.py
- ingest_complaints.py
- ingest_recharges.py

---

# Processed Data Layer

Processed datasets are stored in parquet format for optimized downstream analytics processing.

## Processed Paths

```text
data/processed/customers/
data/processed/calls/
data/processed/complaints/
data/processed/recharges/
```

---

# Snowflake Warehouse Architecture

## Database

```sql
TELECOM_DWH
```

## Schemas

| Schema | Purpose |
|---|---|
| STAGING | Raw warehouse ingestion layer |
| CORE | Cleaned dimensional warehouse layer |
| MART | Business-ready analytics layer |

---

# dbt Transformation Layer

The project uses dbt for modular SQL transformations, warehouse modeling, lineage tracking, and automated testing.

## dbt Features Implemented

- Source definitions
- STAGING models
- CORE warehouse models
- MART analytical models
- Incremental materializations
- Automated data quality tests
- Lineage documentation
- Warehouse dependency management

---

# STAGING Models

- stg_customers
- stg_calls
- stg_complaints
- stg_recharges

---

# CORE Models

## Dimension Tables

- dim_customer

## Fact Tables

- fact_calls
- fact_complaints
- fact_recharges

---

# MART Models

## mart_churn_signals

Business-ready churn analytics mart combining:
- Customer activity
- Recharge behavior
- Complaint severity
- Call activity patterns
- Customer engagement indicators

---

# Incremental Pipeline Architecture

The platform implements incremental processing across ingestion, warehousing, and transformation layers.

## Incremental Features

- Append-only ingestion
- Watermark tracking
- Incremental Spark ingestion
- Incremental parquet processing
- Incremental Snowflake loading
- Incremental dbt transformations

---

# Watermark Tracking

Metadata-based watermark tracking is used to process only newly arrived records.

## Example Metadata Files

```text
metadata/calls_watermark.txt
metadata/snowflake_calls_watermark.txt
```

This simulates production-grade scalable ingestion architecture.

---

# Airflow Orchestration

The platform is fully orchestrated using Apache Airflow.

## DAG Workflow

```text
data_generation_tasks
        ↓
spark_ingestion_tasks
        ↓
snowflake_loading_tasks
        ↓
dbt_transformation_tasks
        ↓
dbt_tests
```

## Airflow Features

- Task Groups
- Dependency management
- Retry handling
- Automated scheduling
- Incremental orchestration
- End-to-end DAG execution

---

# Data Engineering Concepts Implemented

## Distributed Processing

- Apache Spark distributed ingestion pipelines

## Incremental Processing

- Watermark-based ingestion
- Stateful processing
- Append-only pipelines

## Data Warehousing

- Snowflake cloud warehouse
- Layered warehouse architecture
- STAR schema modeling

## Analytics Engineering

- dbt modular transformations
- Incremental dbt models
- Data quality testing
- Lineage documentation

## Orchestration

- Apache Airflow DAG orchestration
- Task dependency management
- Pipeline automation

---

# Data Quality Checks

Implemented data validations include:

- Duplicate removal
- Deduplication logic
- Null handling
- Invalid recharge filtering
- Invalid call duration filtering
- Standardized categorical transformations
- dbt uniqueness tests
- dbt not-null tests

---

# Screenshots

## Airflow DAG Orchestration

```text
screenshots/airflow_dag.png
```

## dbt Lineage Graph

```text
screenshots/dbt_lineage.png
```

## Snowflake Warehouse Tables

```text
screenshots/snowflake_warehouse.png
```

---

# Running the Project

## Start Airflow

```bash
airflow standalone
```

---

## Run Spark Jobs

```bash
spark-submit spark_jobs/jobs/ingest_calls.py
```

---

## Run dbt Models

```bash
dbt run
```

---

## Run dbt Tests

```bash
dbt test
```

---

## Generate dbt Documentation

```bash
dbt docs generate
dbt docs serve
```

---

# Current Project Status

## Completed

- End-to-end Airflow DAG orchestration
- Incremental Spark ingestion pipelines
- Incremental Snowflake warehouse loading
- dbt STAGING, CORE, and MART transformations
- Automated data quality testing
- Watermark-based incremental processing
- Modular warehouse architecture
- Telecom churn analytics mart

---

# Upcoming Enhancements

- SCD Type 2 customer dimension
- CI/CD pipeline automation
- Dockerized deployment
- Pipeline monitoring and alerting

---

# Sample Workflow

```text
Generate Telecom Data
        ↓
Incremental Spark Processing
        ↓
Processed Parquet Layer
        ↓
Incremental Snowflake Loading
        ↓
dbt Warehouse Transformations
        ↓
Business MART Generation
        ↓
Analytics Consumption
```

---

# Conclusion

This project demonstrates a modern end-to-end Data Engineering and Analytics Engineering platform using distributed processing, cloud warehousing, orchestration, incremental processing, and modular warehouse transformations.

The platform follows production-oriented architecture patterns commonly used in modern cloud data platforms.