# Boise Bighorns Analytics (BigQuery + dbt)

This project is an end-to-end analytics warehouse built in **BigQuery** using **dbt** to implement a **Medallion Architecture (Bronze → Silver → Gold)** for a fictional sports franchise (Boise Bighorns).

## Stack
- **BigQuery**: data warehouse
- **dbt Core (dbt-bigquery)**: transformations + modeling
- **VS Code**: development environment
- **GitHub**: version control + portfolio artifact

## Architecture

### Bronze (raw)
- Dataset: `bronze`
- Contains raw ingested tables (ticketing, seat manifest, POS, email, web sessions, etc.)

### Silver (clean + modeled)
- Dataset: `silver_dev`
- **Staging (`stg_` models):**
  - 1:1 models from Bronze
  - Standardized types (IDs, timestamps, amounts)
  - Cleaned column naming
  - Light derived fields (e.g., `*_dt` from timestamps)
- **Core (next):**
  - Business-ready models built from staging (joins, metrics, conformed entities)

### Gold (business layer — planned)
- Executive-ready fact/dimension models and KPI marts for dashboards and reporting

## Repo Structure
- `models/silver/staging/` — staging models (`stg_*`)
- `models/silver/core/` — core models (planned)
- `seeds/` — reference data loaded by dbt (e.g., `date_mapping`)
- `.gitignore` — excludes local env + build artifacts

## How to Run Locally
Prereqs:
- Authenticated `gcloud` + BigQuery access
- `dbt-bigquery` installed in a virtual environment

Common commands:
```bash
dbt debug
dbt run
dbt test
dbt seed