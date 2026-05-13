# Telecom Churn Data Warehouse

## Project Overview

End-to-end Telecom Churn Data Warehouse built using Apache Spark, Snowflake, dbt, and Airflow following modern analytics engineering practices.

This project simulates a real-world telecom analytics platform for customer churn analysis using layered warehouse architecture, distributed data processing, and cloud data warehousing.

The platform processes telecom customer activity data including:
- Customer profiles
- Call usage records
- Complaints
- Recharge transactions

The final goal of the project is to build scalable analytics pipelines and churn analysis marts using modern Data Engineering tools and dimensional modelling techniques.

---

# Architecture

```text
Raw CSV Data
        ↓
Apache Spark Ingestion
        ↓
Processed Parquet Layer
        ↓
Snowflake STAGING Layer
        ↓
dbt Transformations
        ↓
CORE Warehouse Layer
        ↓
MART Analytics Layer
        ↓
Dashboard & Analytics
```
---

# Tech Stack

| Category | Technology |
|---|---|
| Data Processing | Apache Spark, PySpark |
| Cloud Warehouse | Snowflake |
| Transformations | dbt |
| Orchestration | Apache Airflow |
| Programming | Python, SQL |
| Storage Format | Parquet |
| Data Generation | Faker |
| Version Control | Git, GitHub |
| Modelling | Dimensional Modelling, Kimball Star Schema |

---

# Project Structure

```text
telecom-churn-dwh/
│
├── airflow/
├── architecture/
├── dashboards/
├── data/
│   ├── raw/
│   └── processed/
│
├── data_generation/
├── dbt/
├── docs/
├── notebooks/
│
├── spark_jobs/
│   ├── jobs/
│   └── utils/
│
├── scripts/
├── tests/
├── configs/
│
├── README.md
├── requirements.txt
└── .gitignore
```
---

# Data Pipeline Flow

```text
customers.csv
calls.csv
complaints.csv
recharges.csv
        ↓
PySpark Ingestion Jobs
        ↓
Data Cleaning & Validation
        ↓
Processed Parquet Files
        ↓
Snowflake STAGING Tables
        ↓
dbt CORE Models
        ↓
Business MARTS
        ↓
Analytics & Dashboards
```
---

# Current Features

## Data Generation Layer
- Telecom customer data simulation using Faker
- Synthetic datasets generation
- Multiple telecom business entities simulated

Generated datasets:
- customers.csv
- calls.csv
- complaints.csv
- recharges.csv

---

## Spark Ingestion Framework

### Features
- PySpark ingestion jobs
- Schema inference
- Duplicate removal
- Data validation
- Standardized transformations
- Modular Spark architecture
- Reusable Spark session utility
- Structured logging utility

### Spark Jobs
- ingest_customers.py
- ingest_calls.py
- ingest_complaints.py
- ingest_recharges.py

---

# Processed Data Layer

Processed datasets stored in parquet format:

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

The project uses dbt for modular SQL transformations, testing, lineage tracking, and warehouse modelling.

## dbt Features Implemented
- Source definitions
- Staging models
- CORE dimensional models
- MART analytical models
- Incremental materializations
- Data quality tests
- Automatic lineage documentation

---

## CORE Models

### Dimension Tables
- dim_customers

### Fact Tables
- fact_calls
- fact_complaints
- fact_recharges

---

## MART Models

### mart_churn_signals
Business-ready churn analytics mart combining:
- Call activity
- Complaint behavior
- Recharge patterns
- Customer activity indicators

---

# Incremental Pipeline Architecture

The calls pipeline has been upgraded to a fully incremental architecture.

## Incremental Features
- Append-only raw ingestion
- Watermark-based processing
- Incremental Spark ingestion
- Incremental parquet processing
- Incremental Snowflake loading
- Incremental dbt models

---

## Watermark Tracking

The pipeline maintains metadata-based watermark tracking to process only newly arrived records.

Example:
```text
metadata/calls_watermark.txt
metadata/snowflake_calls_watermark.txt
```

This simulates production-grade scalable ingestion architecture.

---

# Data Engineering Concepts Implemented

## Batch Processing
- PySpark batch ingestion pipelines

## Incremental Processing
- Watermark-based ingestion
- Append-only pipelines
- Stateful processing

## Data Warehousing
- Snowflake cloud warehouse
- Layered warehouse architecture
- STAR schema modelling

## Analytics Engineering
- dbt modular transformations
- Data quality testing
- Incremental models
- Lineage documentation

---

# Data Quality Checks

Implemented validations include:
- Duplicate removal
- Null handling
- Invalid recharge filtering
- Invalid call duration filtering
- Standardized categorical fields
- dbt uniqueness tests
- dbt not-null tests

---

# Current Project Status

## Completed
- Data generation pipelines
- Spark ingestion framework
- Snowflake warehouse setup
- dbt warehouse transformations
- Incremental calls pipeline
- CORE and MART models
- dbt lineage and documentation

## In Progress
- Airflow orchestration
- Incremental standardization for all pipelines
- Dashboard layer
- CI/CD automation

---

# Future Enhancements

- Apache Airflow DAG orchestration
- SCD Type 2 implementation
- Snowflake bulk loading using COPY INTO
- Power BI / Streamlit dashboards
- Dockerized deployment
- GitHub Actions CI/CD
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
dbt Transformations
        ↓
Business MART Generation
        ↓
Analytics Consumption
```