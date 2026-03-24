from airflow.sdk import dag, task


@dag(
    dag_id="citibike_elt_pipeline",
    description="Citibike ELT pipeline.",
    schedule=None,
    catchup=False,
    tags=["citibike", "elt", "pipeline"],
)
def citibike_elt_pipeline():
    """
    ### Citibike ELT pipeline.

    Ingest (download CSV ZIP - to GCS) ->
    Load (GCS -> BigQuery) ->
    DBT run (modeling) ->
    DBT test (test models)
    """

    @task()
    def test_task():
        print(
            "THIS IS A TEST TASK. CHECK DAG EXECUTION FOR CONFIRMATION THAT IT WORKS."
        )

    # Task flow
    test_task()


citibike_elt_pipeline()
