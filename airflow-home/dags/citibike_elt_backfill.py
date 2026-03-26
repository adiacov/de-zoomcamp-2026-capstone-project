"""Citibike ELT Backfill Pipeline.

This DAG runs historical backfills of the Citibike ELT pipeline.
It executes the full pipeline (ingest → load → dbt → tests)
for a range of months defined by start and end periods.
The DAG is intended for historical reprocessing and bulk loads,
not for scheduled monthly updates.
"""

from airflow.sdk import DAG, Param, task
from datetime import datetime
from pathlib import Path
from citibike.ingest.citibike_ingest import run_ingest
from citibike.load.citibike_load import run_load


param_start_year = Param(
    default="2024",
    description="Start year of the backfill range.",
    title="Start Year",
    type="string",
    enum=[f"{year}" for year in range(2024, 2026)],
)

param_start_month = Param(
    default="1",
    description="Start month of the backfill range (1–12).",
    title="Start Month",
    type="string",
    enum=[f"{month}" for month in range(1, 13)],
)

param_end_year = Param(
    default="2024",
    description="End year of the backfill range.",
    title="End Year",
    type="string",
    enum=[f"{year}" for year in range(2024, 2026)],
)

param_end_month = Param(
    default="1",
    description="End month of the backfill range (1–12).",
    title="End Month",
    type="string",
    enum=[f"{month}" for month in range(1, 13)],
)

param_bucket = Param(
    default="de_citibike_bucket",
    description="GCS bucket used for storing ingested raw files and as the source for loading.",
    title="GCS Bucket",
    type="string",
)

param_dataset = Param(
    default="de_citibike_raw",
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
    dag_display_name="Citibike ELT Backfill",
    description=__doc__.partition("\n")[0],
    doc_md=__doc__,
    schedule=None,
    catchup=False,
    tags=["citibike", "elt", "backfill"],
    params={
        "start_year": param_start_year,
        "start_month": param_start_month,
        "end_year": param_end_year,
        "end_month": param_end_month,
        "bucket": param_bucket,
        "dataset": param_dataset,
        "force_ingest": param_force_ingest,
    },
) as dag:

    @task(
        task_id="generate_periods",
        task_display_name="Generate Periods",
    )
    def generate_periods(params=None) -> list[dict]:
        """Generate list of (year, month) tuples for the backfill range."""
        periods = []
        start = datetime(int(params["start_year"]), int(params["start_month"]), 1)
        end = datetime(int(params["end_year"]), int(params["end_month"]), 1)
        current = start
        while current <= end:
            periods.append({"year": str(current.year), "month": str(current.month)})
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)
        return periods

    @task(
        task_id="run_ingest",
        task_display_name="Ingest",
        map_index_template="{{ period.year }}-{{ period.month }}",
    )
    def run_ingest_task(period: dict, params=None) -> None:
        """Download Citibike data for the given period and upload it to GCS."""
        run_ingest(
            period["year"], period["month"], params["bucket"], params["force_ingest"]
        )

    @task(
        task_id="run_load",
        task_display_name="Load",
        map_index_template="{{ period.year }}-{{ period.month }}",
    )
    def run_load_task(period: dict, params=None) -> None:
        """Load ingested data from GCS into BigQuery."""
        run_load(period["year"], period["month"], params["bucket"], params["dataset"])

    @task.bash(
        task_id="create_dbt_models",
        task_display_name="DBT Models",
        env={
            "GCP_PROJECT_ID": "{{ var.value.GCP_PROJECT_ID }}",
            "GOOGLE_APPLICATION_CREDENTIALS": "{{ var.value.GOOGLE_APPLICATION_CREDENTIALS }}",
        },
    )
    def run_dbt_models() -> str:
        """Execute DBT models to transform raw data into analytics-ready tables."""
        return "/opt/dbt-venv/bin/dbt run --project-dir /opt/airflow/dbt/citibike --profiles-dir /opt/airflow/dbt/citibike"

    @task.bash(
        task_id="run_dbt_tests",
        task_display_name="DBT Tests",
        env={
            "GCP_PROJECT_ID": "{{ var.value.GCP_PROJECT_ID }}",
            "GOOGLE_APPLICATION_CREDENTIALS": "{{ var.value.GOOGLE_APPLICATION_CREDENTIALS }}",
        },
    )
    def run_dbt_tests() -> str:
        """Run DBT tests to validate data quality and model assumptions."""
        return "/opt/dbt-venv/bin/dbt test --project-dir /opt/airflow/dbt/citibike --profiles-dir /opt/airflow/dbt/citibike"

    periods = generate_periods()

    ingest = run_ingest_task.expand(period=periods)
    load = run_load_task.expand(period=periods)
    dbt_run = run_dbt_models()
    dbt_test = run_dbt_tests()

    ingest >> load >> dbt_run >> dbt_test
