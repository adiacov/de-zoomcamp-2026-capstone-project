"""Citibike ELT pipeline.

This DAG orchestrates a simple ELT workflow for Citibike trip data:
1. Ingest: Download monthly CSV (ZIP) files and upload them to GCS.
2. Load: Load data from GCS into BigQuery (partitioned tables).
3. Transform: Run DBT models to transform raw data.
4. Test: Execute DBT tests to validate transformed models.

All steps are parameterized and share the same runtime configuration.
"""

import os

from airflow.sdk import DAG, Param, task
from citibike.ingest.citibike_ingest import run_ingest
from citibike.load.citibike_load import run_load
from pathlib import Path


GCP_PREFIX = os.getenv("GCP_PREFIX")
if not GCP_PREFIX:
    raise RuntimeError(
        "Environment variable GCP_PREFIX is not set. DAG cannot proceed."
    )

param_year = Param(
    default="2024",
    description="Year of the Citibike dataset to process. Used in both ingest and load steps.",
    title="Year",
    type="string",
    enum=[f"{year}" for year in range(2024, 2026)],
)

param_month = Param(
    default="1",
    description="Month of the Citibike dataset to process (1–12). Used in both ingest and load steps.",
    title="Month",
    type="string",
    enum=[f"{month}" for month in range(1, 13)],
)

param_bucket = Param(
    default=f"{GCP_PREFIX}_citibike_bucket",
    description="GCS bucket used for storing ingested raw files and as the source for loading.",
    title="GCS Bucket",
    type="string",
)

param_dataset = Param(
    default=f"{GCP_PREFIX}_citibike_raw",
    description="Target BigQuery dataset where raw data will be loaded.",
    title="BigQuery Dataset",
    type="string",
)

param_force_ingest = Param(
    default=False,
    description="If True, overwrites existing files in GCS. Useful for recovery or reprocessing.",
    title="Force Re-ingestion",
    type="boolean",
)


with DAG(
    dag_id=Path(__file__).stem,
    dag_display_name="Citibike ELT Pipeline",
    description=__doc__.partition("\n")[0],
    doc_md=__doc__,
    schedule=None,
    catchup=False,
    tags=["citibike", "elt", "pipeline"],
    params={
        "year": param_year,
        "month": param_month,
        "bucket": param_bucket,
        "dataset": param_dataset,
        "force_ingest": param_force_ingest,
    },
) as dag:

    @task(
        task_id="run_ingest",
        task_display_name="Ingest",
    )
    def run_ingest_task(params=None) -> bool:
        """Download Citibike data for the given period and upload it to GCS."""
        return run_ingest(
            params["year"], params["month"], params["bucket"], params["force_ingest"]
        )

    @task(
        task_id="run_load",
        task_display_name="Load",
    )
    def run_load_task(params=None) -> bool:
        """Load ingested data from GCS into BigQuery."""
        return run_load(
            params["year"], params["month"], params["bucket"], params["dataset"]
        )

    @task.bash(
        task_id="dbt_deps",
        task_display_name="DBT Dependencies",
    )
    def run_dbt_deps() -> str:
        """Install dbt packages in the isolated venv."""
        return "/opt/dbt-venv/bin/dbt deps --project-dir /opt/airflow/dbt/citibike --profiles-dir /opt/airflow/dbt/citibike"

    @task.bash(
        task_id="create_dbt_models",
        task_display_name="DBT Models",
    )
    def run_dbt_models() -> str:
        """Execute DBT models to transform raw data into analytics-ready tables."""
        return "/opt/dbt-venv/bin/dbt run --project-dir /opt/airflow/dbt/citibike --profiles-dir /opt/airflow/dbt/citibike"

    @task.bash(
        task_id="run_dbt_tests",
        task_display_name="DBT Tests",
    )
    def run_dbt_tests() -> str:
        """Run DBT tests to validate data quality and model assumptions."""
        return "/opt/dbt-venv/bin/dbt test --project-dir /opt/airflow/dbt/citibike --profiles-dir /opt/airflow/dbt/citibike"

    @task.short_circuit(
        task_id="should_run_dbt",
        task_display_name="Should Run DBT",
    )
    def should_run_dbt(is_ingested: bool, is_loaded: bool) -> bool:
        """Checks if ingest OR load did real work.

        Returns:
        False - no real work was done (no new files downloaded to bucket, or no new raw dataset created).
        True - real work was done (new files downloaded to bucket, or new dataset created).
        """
        return is_ingested or is_loaded

    is_ingested = run_ingest_task()
    is_loaded = run_load_task()
    dbt_deps = run_dbt_deps()
    dbt_run = run_dbt_models()
    dbt_test = run_dbt_tests()

    is_ingested >> is_loaded
    is_more_work = should_run_dbt(is_ingested, is_loaded)
    is_more_work >> dbt_deps >> dbt_run >> dbt_test
