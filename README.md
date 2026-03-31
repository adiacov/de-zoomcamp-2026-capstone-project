# Citibike NYC - Batch ELT Platform

An end-to-end data engineering project built for the [Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp). It ingests Citibike NYC trip data from the public S3 source, loads it into BigQuery, transforms it with dbt, orchestrates the pipeline with Airflow, and visualises the results in a Streamlit dashboard - all running in Docker.

---

## Table of Contents

- [Problem Statement](#problem-statement)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Dataset](#dataset)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [GCP Setup](#gcp-setup)
- [Infrastructure Setup (Terraform)](#infrastructure-setup-terraform)
- [Local Setup](#local-setup)
- [Running the Pipeline](#running-the-pipeline)
- [Airflow DAGs](#airflow-dags)
- [dbt Models](#dbt-models)
- [Dashboard](#dashboard)
- [Design Decisions](#design-decisions)
- [Local Development](#local-development)
- [License](#license)

---

## Problem Statement

Citibike publishes monthly trip CSVs to a public S3 bucket. The raw data is useful but unwieldy - millions of rows spread across dozens of files, with no aggregation and no easy way to answer operational or customer-behaviour questions.

This project builds a production-style pipeline that:

- Reliably ingests and deduplicates raw trip files from S3 into GCS
- Organises data in a BigQuery warehouse with a clear raw → staging → mart layer
- Exposes five analytical mart tables, each built to answer a specific business question
- Presents those answers in an interactive Streamlit dashboard

---

## Architecture

```txt
┌──────────────────────────┐
│        Citibike S3       │  public dataset; published monthly;
└────────────┬─────────────┘
             │  ZIP files
             ▼
┌──────────────────────────┐
│         Ingest           │  metadata driven; avoid duplicate download;
└────────────┬─────────────┘
             │  CSV files
             ▼
┌──────────────────────────┐
│       GCS Bucket         │  files partitioned by year/month;
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│          Load            │  metadata driven; file tracking; retry;
└────────────┬─────────────┘
             │  Raw Data
             ▼
┌──────────────────────────┐
│    BigQuery - Raw        │  trips partitioned daily;
└────────────┬─────────────┘
             │  DBT (materialization: view)
             ▼
┌──────────────────────────┐
│   BigQuery - Staging     │  cleansing; transformation;
└────────────┬─────────────┘
             │  DBT (materialization: table)
             ▼
┌──────────────────────────┐
│   BigQuery - Marts       │  aggregations;
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│       Dashboard          │  Streamlit + Plotly
└──────────────────────────┘
```

The full pipeline is orchestrated by Airflow. Two DAGs are provided: a single-month ELT pipeline and a multi-month backfill pipeline.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Infrastructure | Terraform |
| Cloud storage | Google Cloud Storage |
| Data warehouse | BigQuery |
| Ingestion & loading | Python |
| Orchestration | Apache Airflow |
| Transformation | DBT |
| Dashboard | Streamlit + Plotly |
| Dependency management | uv |
| Containerization | Docker |

---

## Dataset

**Source:** [Citibike System Data](https://citibikenyc.com/system-data) - publicly available on S3 at `s3://tripdata/`.

Each monthly file contains one row per trip with fields including: ride ID, rideable type, start/end timestamps, start/end station name and coordinates, and member vs. casual rider type.

The pipeline processes data per month and year, configurable at DAG trigger time. Supported range: **2024–2025**.

> **Data size warning:** Individual monthly source files can exceed 200 MB. Process one month at a time when getting started. Be especially careful with the backfill DAG - a full year triggers downloads and BigQuery loads for all 12 months simultaneously.

---

## Project Structure

```txt
project-root/
├── airflow-home/               # Airflow DAG definitions
|   |── dags/
│   ├── citibike_elt_pipeline.py
│   └── citibike_elt_backfill.py
├── dbt/citibike/
│   ├── models/
│   │   ├── staging/            # staging views
│   │   └── marts/              # mart tables
│   └── dbt_project.yml
├── dashboard/
│   ├── app.py                  # Streamlit entry point
│   ├── charts.py               # Plotly chart definitions
│   └── data.py                 # BigQuery query helpers
├── src/citibike/
│   ├── ingest/citibike_ingest.py
│   └── load/citibike_load.py
├── terraform/
|   ├── main.tf
│   └── variables.tf
├── dev/                        # Gitignored - holds credentials.json
├── Dockerfile.airflow
├── Dockerfile.streamlit
├── docker-compose.yml
├── pyproject.toml
├── uv.lock
├── Makefile
├── .env                        # Gitignored - local development
├── .env.docker                 # Committed - safe, no secrets
└── sample.env                  # Commited - template for local .env
```

---

## Prerequisites

Install the following before proceeding:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
- [Terraform](https://developer.hashicorp.com/terraform/install) ≥ 1.5
- A [Google Cloud Platform](https://console.cloud.google.com/) account

> **Windows users:** This project was developed on macOS/Linux. On Windows, run all commands inside [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) with Docker Desktop's WSL2 backend enabled.

---

## Quick Start

A full walkthrough of each step is available in the sections below. This is the condensed version for reference.

**1. Clone the repository**

```bash
git clone https://github.com/adiacov/de-zoomcamp-2026-capstone-project.git
cd de-zoomcamp-2026-capstone-project
```

**2. Set up GCP** - create a project, enable APIs, create a service account with BigQuery Admin + Storage Admin roles, download the credentials JSON

```
dev/credentials.json   ← place the downloaded file here (gitignored)
```

> Note: Full instructions: [GCP Setup](#gcp-setup)

**3. Configure Terraform** - open `terraform/variables.tf` and set your project ID and prefix

```hcl
variable "project" { default = "your-gcp-project-id" }   # ← your GCP project ID
variable "prefix"  { default = "your-prefix" }            # ← unique prefix (e.g. your project ID)
```

**4. Provision infrastructure**

```bash
make tf-init
make tf-apply
```

> Full instructions: [Infrastructure Setup (Terraform)](#infrastructure-setup-terraform)

**5. Configure environment files** - set `GCP_PROJECT_ID` and `GCP_PREFIX` in both files

```bash
cp sample.env .env   # then edit .env; also edit .env.docker
```

> Full instructions: [Local Setup](#local-setup)

**6. Build and start the stack**

```bash
make build   # once; it'l take some time
make up
```

**7. Trigger a DAG run** - open [localhost:8080](http://localhost:8080), log in (`airflow` / `airflow`), trigger a DAG, set parameters

> Full instructions: [Running the Pipeline](#running-the-pipeline)

**8. View the dashboard** - open [localhost:8501](http://localhost:8501) after the first DAG run completes

---

## GCP Setup

### 1. Create a GCP project

Go to the [GCP Console](https://console.cloud.google.com/projectcreate) and create a new project. Note the **Project ID** - you will need it throughout this setup.

### 2. Enable required APIs

Enable the following APIs in your project:

- [Cloud Storage API](https://console.cloud.google.com/apis/library/storage.googleapis.com)
- [BigQuery API](https://console.cloud.google.com/apis/library/bigquery.googleapis.com)
- [IAM API](https://console.cloud.google.com/apis/library/iam.googleapis.com)

### 3. Create a service account

1. Go to **IAM & Admin → Service Accounts → Create Service Account**
2. Give it a name (e.g., `de-citibike-sa`)
3. Grant the **BigQuery Admin** and **Storage Admin** roles
4. Click **Done**

### 4. Download the credentials JSON

1. Open the service account you just created
2. Go to the **Keys** tab → **Add Key → Create new key → JSON**
3. Download the file and place it at:

   ```
   dev/credentials.json
   ```

   This path is gitignored. Never commit this file.

---

## Infrastructure Setup (Terraform)

Terraform provisions the GCS bucket and BigQuery datasets. Run this **once** before starting Docker.

### 1. Set your project ID and prefix in `variables.tf`

Open `terraform/variables.tf` and update these two values:

```hcl
variable "project" {
  description = "GCP project ID"
  default     = "your-gcp-project-id"   # ← replace with your GCP project ID
}

variable "prefix" {
  description = "Unique prefix for all GCP resource names"
  default     = "your-prefix"           # ← replace with a unique value (e.g. your GCP project ID)
}
```

`GCP_PREFIX` is used to construct all GCP resource names automatically:

| Resource | Name |
|---|---|
| GCS bucket | `{prefix}_citibike_bucket` |
| BigQuery dataset (raw) | `{prefix}_citibike_raw` |
| BigQuery dataset (staging) | `{prefix}_citibike_staging` |
| BigQuery dataset (marts) | `{prefix}_citibike_marts` |

> **Why a prefix?** GCS bucket names are globally unique across all of Google Cloud - a bucket named `citibike_bucket` almost certainly already exists in someone else's project. Prefixing with your GCP project ID or another unique identifier guarantees no collision. BigQuery dataset names are scoped to your project and don't have this problem, but the prefix is applied consistently everywhere for clarity.

### 2. Provision infrastructure

```bash
make tf-init
make tf-apply
```

Review the plan and type `yes` to confirm. Terraform will create the GCS bucket and all three BigQuery datasets.

Once Terraform completes, use the bucket name `{GCP_PREFIX}-citibike-bucket` when setting DAG parameters at trigger time (see [Running the Pipeline](#running-the-pipeline)).

---

## Local Setup

### 1. Create your local environment file

```bash
cp sample.env .env
```

Open `.env` and fill in the two marked values:

```dotenv
##### GCP #####
GOOGLE_APPLICATION_CREDENTIALS=dev/credentials.json
GCP_PROJECT_ID=<gcp project id>    # ← your GCP project ID
GCP_PREFIX=<gcp prefix>            # ← unique prefix for all GCP resource names
                                   #   (e.g. your GCP project ID)
##### AIRFLOW #####
AIRFLOW_PROJ_DIR=./airflow-home
AIRFLOW_UID=1000
```

This file is used for local development only (running scripts locally, Terraform). It is gitignored and never committed.

### 2. Configure the Docker environment file

Open `.env.docker` and fill in the two marked values:

```dotenv
##### GCP #####
GOOGLE_APPLICATION_CREDENTIALS=/opt/gcp/credentials.json   # container path - do not change
GCP_PROJECT_ID=<gcp project id>    # ← your GCP project ID
GCP_PREFIX=<gcp prefix>            # ← same value as in .env
```

`.env.docker` is committed to git and contains no secrets. The credentials JSON is injected into containers at runtime via a Docker volume mount (`dev/credentials.json` → `/opt/gcp/credentials.json`, read-only).

---

## Running the Pipeline

### 1. Build Docker images

Run this once after cloning (and again after any changes to `Dockerfile.airflow` or `Dockerfile.streamlit`):

```bash
make build
```

### 2. Start the full stack

```bash
make up
```

This starts:

- **Airflow** webserver + scheduler - [localhost:8080](http://localhost:8080)
- **Streamlit** dashboard - [localhost:8501](http://localhost:8501)

Default Airflow login: `airflow` / `airflow`

### 3. Trigger a DAG run

Both DAGs have `schedule=None` and must be triggered manually.

1. Open [localhost:8080](http://localhost:8080) and log in
2. Find the DAG you want to run (see [Airflow DAGs](#airflow-dags) below)
3. Click the **Trigger** button
4. Set the parameters in the UI - at minimum, update `bucket` to match the bucket name created by Terraform

### 4. Stop the stack

```bash
make down
```

---

## Airflow DAGs

### `citibike_elt_pipeline` - Standard ELT Pipeline

Processes a single month of Citibike data end-to-end.

**Task flow:**

```
run_ingest → run_load → should_run_dbt → install_dbt_deps → create_dbt_models → run_dbt_tests
```

`should_run_dbt` is a short-circuit gate: if neither ingest nor load produced new data, the dbt steps are skipped entirely.

**Parameters:**

| Parameter | Default | Description |
|---|---|---|
| `year` | `2024` | Year to process (2024–2025) |
| `month` | `1` | Month to process (1–12) |
| `bucket` | `{prefix}_citibike_bucket` | Your GCS bucket name - must match what Terraform created |
| `dataset` | `{prefix}_citibike_raw` | Target BigQuery dataset |
| `force_ingest` | `false` | If true, bypasses the ETag cache and re-validates GCS state |

> **Start small.** Each monthly source file can exceed 200 MB. Run a single month first to verify the full pipeline works before processing more data.

---

### `citibike_elt_backfill` - Backfill Pipeline

Processes a range of months in a single run. Use this for historical loads, not for regular monthly updates.

**Task flow:**

```
generate_periods → run_ingest (mapped) → run_load (mapped) → install_dbt_deps → create_dbt_models → run_dbt_tests
```

Ingest and load tasks are dynamically mapped - one task instance is created per month in the specified range.

**Parameters:**

| Parameter | Default | Description |
|---|---|---|
| `start_year` | `2024` | Start year of the backfill range |
| `start_month` | `1` | Start month of the backfill range |
| `end_year` | `2024` | End year of the backfill range |
| `end_month` | `1` | End month of the backfill range |
| `bucket` | `{prefix}_citibike_bucket` | Your GCS bucket name - must match what Terraform created |
| `dataset` | `{prefix}_citibike_raw` | Target BigQuery dataset |
| `force_ingest` | `false` | If true, bypasses the ETag cache and re-validates GCS state |

> **Data volume warning:** The backfill DAG processes all months in the range in parallel. A full year means 12 concurrent downloads (each 200 MB+) and 12 BigQuery load jobs. Start with a short range (2-3 months) to validate before running a full backfill.

---

## dbt Models

dbt models live in `dbt/citibike/models/`.

### Staging

| Model | Materialisation | Description |
|---|---|---|
| `stg_trips` | View | Cleans and casts raw trip data from `{prefix}_citibike_raw.trips` |

### Marts

All marts are materialised as **tables** for dashboard query stability.

| Model | Grain | Description |
|---|---|---|
| `mart_trips_by_hour` | date + hour + customer_type + rideable_type | Trip volume by time of day |
| `mart_trips_by_day_type` | day_type + customer_type + rideable_type | Weekday vs. weekend patterns |
| `mart_station_activity` | station_name + station_role | Top 10 start and end stations |
| `mart_trips_by_month` | year + month + customer_type + rideable_type | Monthly trip volume trends |
| `mart_avg_duration_by_customer` | customer_type + rideable_type | Average and median trip duration in minutes |

---

## Dashboard

The Streamlit dashboard queries the mart tables directly from BigQuery using the same service account credentials.

Open [localhost:8501](http://localhost:8501). The pipeline must have completed at least one successful run before the dashboard has data to display.

**Sections:**

- **Overview** - total trips, rideable mix, monthly trends
- **Operations** - hourly and day-type patterns, top station activity
- **Customer Insights** - member vs. casual behaviour, trip duration distributions

All charts are built with Plotly.

---

## Design Decisions

**ETag-based ingest caching.** The ingest script performs a single cheap HTTP HEAD request to check the remote file's ETag before doing anything else. If the ETag matches the local cache (`.ingestion_cache.json`), the entire download is skipped. If the ETag has changed - or if no cache exists - the script downloads the ZIP and uploads only the inner CSV files that are missing or have a different size in GCS. This means re-running ingest for a month you've already processed costs almost nothing. The `force_ingest` parameter bypasses the ETag check while still applying the per-file GCS size check, making it safe to use for repair without wasting bandwidth.

**`_loaded_files` tracking table.** The load script maintains a `_loaded_files` metadata table in BigQuery that records every GCS URI it has successfully loaded. On subsequent runs, already-loaded files are skipped before any BigQuery jobs are started, preventing double-counting in the raw table.

**Short-circuit gate before dbt.** The standard ELT DAG includes a `should_run_dbt` task that skips the dbt steps entirely if neither ingest nor load produced new data. This avoids unnecessary model rebuilds on re-runs where nothing changed.

**Marts as tables, not views.** Dashboard queries run against pre-aggregated tables so load time is consistent regardless of upstream data volume.

**Median alongside average for trip duration.** `mart_avg_duration_by_customer` exposes both `avg_trip_duration_minutes` and `median_trip_duration_minutes`. For casual classic bike riders, the mean is 24.9 minutes but the median is 12.3 minutes - a gap large enough to mislead if only the average is shown. Both metrics are surfaced in the dashboard.

**Single `mart_station_activity` with a `station_role` column.** Rather than two separate marts (one for start stations, one for end stations), a single mart uses a `station_role` dimension (`start` / `end`). This halves the mart count and simplifies dashboard queries.

**Isolated dbt virtualenv in `Dockerfile.airflow`.** dbt-core and Airflow share a dependency on `protobuf` but require incompatible versions. dbt runs inside its own Python virtualenv within the Airflow container, keeping the two dependency trees separate and avoiding import errors at runtime.

---

## Local Development

The Makefile exposes targets for running pipeline steps locally without Docker, using `uv`. These are useful for debugging individual steps outside of Airflow.

```bash
make ingest         # Run the ingest script locally (uses defaults from the script)
make load           # Run the load script locally (uses defaults from the script)
make streamlit-run  # Run the Streamlit dashboard locally
```

These use the credentials and project ID from your local `.env` file. Check the script implementations in `src/citibike/` for available CLI arguments if you need to override the defaults (e.g., year, month, bucket, or `--force` for ingest).

---

## License

This project is licensed under the [MIT License](LICENSE).
