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
