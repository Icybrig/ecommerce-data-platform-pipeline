# E-commerce Data Platform Pipeline

An end-to-end data engineering project built with **PySpark, Delta Lake, Docker, Airflow, and GitHub Actions**, following the **Medallion Architecture**.

The project simulates an e-commerce data platform that ingests raw transactional data, applies data quality rules and transformations, processes incremental updates through CDC, and produces business-ready analytical datasets.

> **Project status:** Local PySpark + Delta Lake pipeline implemented. Data quality tests and CI/CD implemented. Airflow orchestration is currently being integrated. AWS S3 and Databricks deployment are planned as the next stage.

---

## Architecture

```text
                         ┌─────────────────────┐
                         │    Raw CSV Data     │
                         │ customers / orders  │
                         │      / products     │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │   Bronze Layer     │
                         │      Delta Lake     │
                         │ Raw + metadata      │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    Silver Layer    │
                         │ Data Cleaning      │
                         │ Type Casting       │
                         │ Deduplication      │
                         │ Data Quality       │
                         └──────────┬──────────┘
                                    │
                         ┌──────────▼──────────┐
                         │     CDC / MERGE     │
                         │ Incremental Updates │
                         │ Watermark + Lookback│
                         │ Late-arriving Data  │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │     Gold Layer     │
                         │ Business Analytics │
                         └──────────┬──────────┘
                                    │
                   ┌────────────────┼────────────────┐
                   ▼                ▼                ▼
            Daily Sales      Customer Sales    Product Sales


             Orchestration / Automation
             ──────────────────────────
             Airflow
                │
                ├── Bronze
                ├── Silver
                ├── CDC / MERGE
                └── Gold

             CI/CD
             ─────
             GitHub Actions
                    │
                    ▼
              Automated Tests
```

---

## Project Overview

The objective of this project is to build a realistic **modern data platform pipeline** rather than a collection of isolated Spark exercises.

The pipeline demonstrates how raw e-commerce data can be transformed into reliable analytical datasets while handling common data engineering challenges such as:

* Data quality validation
* Duplicate records
* Incremental processing
* Change Data Capture (CDC)
* Late-arriving data
* Idempotent updates
* Delta Lake transactions
* Medallion Architecture
* Automated testing
* CI/CD

---

## Technology Stack

| Technology         | Role                              |
| ------------------ | --------------------------------- |
| **Python**         | Pipeline development              |
| **PySpark**        | Distributed data processing       |
| **Delta Lake**     | Transactional data storage        |
| **SQL**            | Data transformation and analytics |
| **Docker**         | Airflow and local infrastructure  |
| **Airflow**        | Pipeline orchestration            |
| **Git / GitHub**   | Version control                   |
| **GitHub Actions** | CI/CD and automated testing       |
| **AWS S3**         | Planned cloud storage             |
| **Databricks**     | Planned cloud data platform       |

---

## Data Model

The project uses three main source datasets.

### Customers

Contains customer information.

```text
customer_id
name
country
signup_date
updated_at
```

### Products

Contains product information.

```text
product_id
product_name
category
price
updated_at
```

### Orders

Contains transactional order data.

```text
order_id
customer_id
product_id
quantity
amount
order_date
updated_at
```

The sample dataset contains:

* 100 customers
* 50 products
* 1,000 initial orders

Additional CDC records are generated to simulate updates, inserts, duplicates, and late-arriving events.

---

# Medallion Architecture

## Bronze Layer

The Bronze layer stores the raw source data in Delta format.

The ingestion process:

1. Reads CSV files from `data/raw/`
2. Infers the source schema
3. Adds ingestion metadata
4. Writes the result as Delta tables

Example metadata columns:

```text
_ingested_at
_source
```

Generated tables:

```text
data/bronze/customers
data/bronze/products
data/bronze/orders
```

The Bronze layer preserves the source data while adding technical metadata needed for downstream processing.

---

## Silver Layer

The Silver layer contains cleaned and standardized datasets.

The transformation process includes:

* Explicit data type casting
* Null checks
* Positive quantity validation
* Positive amount validation
* Business-key deduplication
* Latest-record selection using `updated_at`
* Preservation of ingestion metadata

For example, duplicate order records are resolved by keeping the most recent record:

```text
order_id = 1001

11:00 → amount = 150
12:00 → amount = 180

Result:
12:00 → amount = 180
```

This ensures that the Silver layer contains one current record per business key.

---

# Change Data Capture

The project implements a simulated **CDC pipeline** for incremental order processing.

CDC records can represent:

* New orders
* Updated orders
* Duplicate events
* Late-arriving records

The CDC process uses Delta Lake `MERGE`:

```text
                 CDC source
                     │
                     ▼
              Data validation
                     │
                     ▼
              Deduplication
                     │
                     ▼
              Watermark filter
                     │
                     ▼
                Delta MERGE
                /          \
               /            \
        Existing row      New row
             │                │
             ▼                ▼
         UPDATE           INSERT
```

The merge condition is based on `order_id`.

Existing records are updated only when the incoming `updated_at` is newer than the current record.

Conceptually:

```text
target.order_id = source.order_id
AND
source.updated_at > target.updated_at
```

This makes the pipeline **idempotent**: replaying the same CDC event does not incorrectly modify the target table.

---

# Watermark and Late-Arriving Data

The CDC pipeline maintains a watermark representing the latest successfully processed event timestamp.

```text
Current watermark
        │
        ▼
2026-09-16 12:00
```

Instead of processing only events strictly after the watermark, the pipeline uses a small **lookback window**.

```text
Watermark
12:00
  │
  │ 5-minute lookback
  ▼
11:55 ─────────────────── 12:00
```

This allows the pipeline to capture events that arrive late.

For example:

```text
Watermark = 12:00

Late event:
updated_at = 11:58
```

Although the event is older than the watermark, it falls inside the lookback window and is therefore processed.

The Delta `MERGE` then determines whether the record should actually be inserted or updated.

This combination provides:

* Incremental processing
* Late-event tolerance
* Idempotency
* Protection against duplicate processing

---

# Gold Layer

The Gold layer contains business-oriented analytical datasets.

Three Gold tables are currently implemented.

## Daily Sales

Aggregates sales by order date.

Metrics include:

```text
order_date
total_orders
total_quantity
total_revenue
```

Example analytical questions:

* How many orders were placed each day?
* What was the daily revenue?
* How many products were sold?

---

## Customer Sales

Joins orders with customer information and aggregates sales by customer.

Metrics include:

```text
customer_id
name
country
total_orders
total_quantity
total_revenue
```

This dataset can support analysis such as:

* Revenue by customer
* Orders by country
* Customer purchasing activity

---

## Product Sales

Joins orders with product information and aggregates sales by product.

Metrics include:

```text
product_id
product_name
category
total_orders
total_quantity
total_revenue
```

This dataset can support:

* Product performance analysis
* Revenue by category
* Product sales volume

---

# Data Quality

The project includes automated Silver-layer data quality tests using **pytest**.

Current checks include:

* No null `order_id`
* `quantity > 0`
* `amount > 0`
* No duplicate `order_id`
* No duplicate `customer_id`
* No duplicate `product_id`

Tests can be executed locally with:

```bash
pytest tests/ -v
```

Example result:

```text
6 passed
```

The tests use a shared PySpark fixture defined in:

```text
tests/conftest.py
```

---

# CI/CD

GitHub Actions automatically runs the data quality test suite when changes are pushed to the repository.

Current CI workflow:

```text
Git Push
   │
   ▼
GitHub Actions
   │
   ├── Set up Python
   ├── Install dependencies
   ├── Install PySpark / Delta Lake
   └── Run pytest
            │
            ▼
       Test Results
```

Workflow file:

```text
.github/workflows/ci.yml
```

This ensures that changes to the pipeline are automatically validated before being integrated.

---

# Airflow Orchestration

Airflow is being integrated to orchestrate the complete pipeline.

Target DAG:

```text
Bronze Ingestion
       │
       ▼
Silver Transformation
       │
       ▼
CDC / MERGE
       │
       ├──────────────┐
       ▼              ▼
Daily Sales      Customer Sales
       │
       └──────┬───────┘
              ▼
        Product Sales
```

The Airflow DAG is located at:

```text
airflow/dags/ecommerce_pipeline.py
```

The current DAG uses `BashOperator` to execute the existing PySpark pipeline scripts.

Airflow infrastructure is containerized with Docker Compose:

```text
airflow/docker-compose.yml
```

The local Airflow setup contains:

* Airflow Webserver
* Airflow Scheduler
* PostgreSQL metadata database

> Airflow orchestration is currently under integration. The pipeline itself can already be executed directly through the PySpark scripts.

---

# Project Structure

```text
ecommerce-data-platform-pipeline/
│
├── README.md
│
├── architecture/
│
├── data/
│   ├── raw/
│   └── sample/
│
├── src/
│   ├── bronze/
│   │   └── ingest.py
│   │
│   ├── silver/
│   │   ├── transform.py
│   │   └── merge_orders.py
│   │
│   ├── gold/
│   │   ├── daily_sales.py
│   │   ├── customer_sales.py
│   │   └── product_sales.py
│   │
│   └── utils/
│       ├── generate_data.py
│       └── generate_cdc.py
│
├── tests/
│   ├── conftest.py
│   └── test_silver_quality.py
│
├── airflow/
│   ├── dags/
│   │   └── ecommerce_pipeline.py
│   └── docker-compose.yml
│
├── config/
│
├── notebooks/
│
├── requirements.txt
├── Dockerfile
└── .gitignore
```

Generated Delta data is intentionally excluded from Git through `.gitignore`.

---

# Running the Project Locally

## 1. Clone the repository

```bash
git clone https://github.com/Icybrig/ecommerce-data-platform-pipeline.git
cd ecommerce-data-platform-pipeline
```

## 2. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

## 3. Install dependencies

```bash
pip install -r requirements.txt
```

## 4. Generate sample data

```bash
python src/utils/generate_data.py
```

This creates:

```text
data/raw/
├── customers.csv
├── orders.csv
└── products.csv
```

## 5. Run Bronze ingestion

```bash
python src/bronze/ingest.py
```

## 6. Run Silver transformation

```bash
python src/silver/transform.py
```

## 7. Generate CDC data

```bash
python src/utils/generate_cdc.py
```

## 8. Run CDC / MERGE

```bash
python src/silver/merge_orders.py
```

## 9. Generate Gold datasets

```bash
python src/gold/daily_sales.py
python src/gold/customer_sales.py
python src/gold/product_sales.py
```

## 10. Run data quality tests

```bash
pytest tests/ -v
```

---

# Key Engineering Concepts Demonstrated

This project focuses on practical data engineering concepts commonly used in modern data platforms.

### Distributed Processing

* PySpark DataFrames
* Transformations and actions
* Joins
* Aggregations
* Partition-based processing

### Data Architecture

* Data Lake
* Medallion Architecture
* Bronze / Silver / Gold
* Lakehouse concepts

### Data Reliability

* Data quality validation
* Deduplication
* Idempotency
* Incremental processing
* Late-arriving data

### Delta Lake

* ACID transactions
* Delta tables
* `MERGE`
* Transaction log
* Incremental updates

### Pipeline Engineering

* Airflow DAGs
* Task dependencies
* Docker
* Automated testing
* GitHub Actions

---

# Cloud Architecture — Planned

The local implementation is designed to be migrated to an AWS + Databricks environment.

Target architecture:

```text
                     AWS
                      │
             ┌────────▼────────┐
             │     S3 Raw      │
             └────────┬────────┘
                      │
                      ▼
             ┌─────────────────┐
             │    Databricks   │
             │      Spark      │
             └────────┬────────┘
                      │
              ┌───────▼───────┐
              │ Bronze Delta  │
              └───────┬───────┘
                      │
              ┌───────▼───────┐
              │ Silver Delta  │
              └───────┬───────┘
                      │
              ┌───────▼───────┐
              │  Gold Delta   │
              └───────┬───────┘
                      │
                      ▼
                 BI / Analytics
```

The planned cloud version will introduce:

* AWS S3
* Databricks
* Delta Lake on cloud storage
* Databricks Workflows
* Cloud-based Spark processing
* Production-style data ingestion

---

# Roadmap

## Completed

* [x] Project structure
* [x] Sample e-commerce data generation
* [x] PySpark Bronze ingestion
* [x] Delta Lake Bronze tables
* [x] Silver data transformation
* [x] Data quality filtering
* [x] Deduplication
* [x] CDC simulation
* [x] Delta `MERGE`
* [x] Watermark processing
* [x] Lookback window
* [x] Late-arriving event handling
* [x] Idempotent CDC processing
* [x] Gold daily sales
* [x] Gold customer sales
* [x] Gold product sales
* [x] Pytest data quality tests
* [x] GitHub Actions CI
* [x] Docker-based Airflow setup

## In Progress

* [ ] Complete Airflow orchestration
* [ ] Airflow pipeline integration testing
* [ ] Improve Docker Compose health checks
* [ ] Pipeline monitoring and logging

## Planned

* [ ] AWS S3 integration
* [ ] Databricks deployment
* [ ] Databricks Workflows
* [ ] Delta Lake optimization
* [ ] Partition pruning
* [ ] Data skipping / Z-ORDER
* [ ] `OPTIMIZE`
* [ ] `VACUUM`
* [ ] Production-style configuration management
* [ ] Monitoring and alerting
* [ ] Cloud deployment documentation

---

# Why This Project?

This project was built to demonstrate how a data engineering pipeline evolves from a simple local data processing workflow into a more production-oriented data platform.

The focus is not only on transforming data, but also on solving practical engineering problems:

```text
Raw Data
   ↓
How do we ingest it?
   ↓
How do we validate it?
   ↓
How do we remove duplicates?
   ↓
How do we process only new changes?
   ↓
What happens when data arrives late?
   ↓
How do we make retries safe?
   ↓
How do we test the pipeline?
   ↓
How do we orchestrate it?
   ↓
How do we deploy it to the cloud?
```

The final goal is to evolve the project into a production-style **AWS + Databricks data platform**.