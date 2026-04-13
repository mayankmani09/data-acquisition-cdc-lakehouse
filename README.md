# data-acquisition-cdc-lakehouse
A production-style data engineering platform that ingests CDC from multiple transactional systems during a post-acquisition integration, standardizes it into governed Bronze/Silver/Gold data products, and serves both analytics and operational use cases.

## Overview

When companies acquire another business, one of the hardest data engineering problems is integrating multiple transactional systems without breaking downstream analytics, SLAs, or governance. This project demonstrates a contract-first, medallion-style lakehouse that ingests historical and incremental data from multiple source systems, standardizes it into conformed domain models, and publishes analytics-ready Gold datasets.

This repository is designed to showcase senior-level data engineering capabilities:
- CDC ingestion and merge logic
- Bronze / Silver / Gold lakehouse design
- PySpark transformations for conformance and survivorship
- dbt marts, contracts, tests, and documentation
- Airflow orchestration
- observability, data quality, and CI/CD
- business-aligned modeling for acquisition integration

## Business scenario

A parent company acquires another business. Each company has its own:
- customer systems
- product catalogs
- order schemas
- payment and refund models
- key structures and data conventions

The goal is to:
- ingest full historical load plus incremental CDC
- handle schema drift and inconsistent source semantics
- standardize shared domains like customers, orders, products, and payments
- create trusted Gold marts for executive and operational reporting
- provide strong lineage, testing, and operational visibility

## Architecture

### Source systems
- Source A: PostgreSQL-like OLTP system
- Source B: MySQL / SQL Server-like OLTP system
- Reference/API inputs: currency rates, region mappings, product classifications

### Ingestion
- Historical full load
- Incremental CDC events
- S3-style raw landing format
- metadata fields such as:
  - `source_system`
  - `op`
  - `event_ts`
  - `ingest_ts`
  - `batch_id`

### Storage and processing
- Raw landing zone -> Bronze
- Conformed domain layer -> Silver
- Analytics marts -> Gold

### Technology stack
- Python
- PySpark
- SQL
- dbt
- Apache Airflow
- Delta / lakehouse-style tables
- AWS-aligned architecture patterns
- GitHub Actions
- Great Expectations or Soda for data quality

## Medallion layers

### Bronze
Raw landed data from source systems and APIs with minimal transformation.

Examples:
- `bronze.source_a_customers`
- `bronze.source_b_orders`
- `bronze.currency_rates_raw`

### Silver
Validated, deduplicated, conformed domain entities with standardized datatypes, keys, and business rules.

Examples:
- `silver.customers_conformed`
- `silver.orders_conformed`
- `silver.products_conformed`
- `silver.payments_conformed`

### Gold
Business-facing marts optimized for analytics and operational reporting.

Examples:
- `gold.customer_360`
- `gold.daily_revenue`
- `gold.order_margin`
- `gold.acquisition_data_quality_scorecard`

## Core capabilities

### CDC and merge handling
- full load + incremental update support
- insert / update / delete semantics
- deduplication and late-arriving record handling
- survivorship logic across systems
- source precedence rules

### Data quality and contracts
- schema validation
- row-count reconciliation
- null / uniqueness / referential integrity checks
- dbt contracts for critical models
- quarantining invalid records

### Observability
- pipeline run metadata
- task durations
- freshness checks
- data quality results
- schema drift logging
- failure notifications

### CI/CD
- Python linting and tests
- dbt parse/build/test
- sample Spark integration tests
- automated validation on pull requests

## Repository structure

```text
acquisition-cdc-lakehouse/
├── README.md
├── architecture/
├── data/
├── dags/
├── dbt/
├── src/
├── tests/
├── infra/
├── notebooks/
├── docs/
├── .github/workflows/
├── docker-compose.yml
├── requirements.txt
└── Makefile
