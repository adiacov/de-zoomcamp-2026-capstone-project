import os
import time
import random
import argparse
from datetime import datetime, timezone

from google.cloud import bigquery, storage
from dotenv import load_dotenv

load_dotenv()

RAW_TABLE = "trips"
META_TABLE = "_loaded_files"

# -------------------- LOG --------------------


def log_info(msg):
    print(f"[INFO] {msg}")


def log_error(msg):
    print(f"[ERROR] {msg}")


# -------------------- RETRY --------------------


def retry(fn, retries=3, base_delay=1):
    """Simple exponential backoff retry."""
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            if i == retries - 1:
                raise
            sleep = base_delay * (2**i) + random.random()
            log_info(f"RETRY attempt={i+1} sleep={sleep:.2f}s error={e}")
            time.sleep(sleep)


# -------------------- GCP --------------------


def get_bq_client():
    """Init BigQuery client."""
    return bigquery.Client()


def get_gcs_client():
    """Init GCS client."""
    return storage.Client()


# -------------------- METADATA --------------------


def ensure_meta_table(bq, dataset):
    """Create _loaded_files table if it doesn't exist."""
    table_ref = f"{bq.project}.{dataset}.{META_TABLE}"
    schema = [
        bigquery.SchemaField("gcs_uri", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    bq.create_table(table, exists_ok=True)
    log_info(f"META_TABLE_READY table={table_ref}")


def load_loaded_uris(bq, dataset):
    """Return set of GCS URIs already loaded."""
    query = f"SELECT gcs_uri FROM `{bq.project}.{dataset}.{META_TABLE}`"
    rows = bq.query(query).result()
    uris = {row.gcs_uri for row in rows}
    log_info(f"ALREADY_LOADED count={len(uris)}")
    return uris


def mark_loaded(bq, dataset, gcs_uri):
    """Record a successfully loaded URI into _loaded_files."""
    table_ref = f"{bq.project}.{dataset}.{META_TABLE}"
    rows = [{"gcs_uri": gcs_uri, "loaded_at": datetime.now(timezone.utc).isoformat()}]
    errors = bq.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"Failed to mark URI as loaded: {errors}")
    log_info(f"META_MARKED uri={gcs_uri}")


# -------------------- BQ TABLE --------------------


def ensure_raw_table(bq, dataset):
    """Create citibike_trips_raw partitioned by day on started_at if not exists."""
    table_ref = f"{bq.project}.{dataset}.{RAW_TABLE}"
    schema = [
        bigquery.SchemaField("ride_id", "STRING"),
        bigquery.SchemaField("rideable_type", "STRING"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("ended_at", "TIMESTAMP"),
        bigquery.SchemaField("start_station_name", "STRING"),
        bigquery.SchemaField("start_station_id", "STRING"),
        bigquery.SchemaField("end_station_name", "STRING"),
        bigquery.SchemaField("end_station_id", "STRING"),
        bigquery.SchemaField("start_lat", "FLOAT"),
        bigquery.SchemaField("start_lng", "FLOAT"),
        bigquery.SchemaField("end_lat", "FLOAT"),
        bigquery.SchemaField("end_lng", "FLOAT"),
        bigquery.SchemaField("member_casual", "STRING"),
    ]
    partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="started_at",
    )
    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = partitioning
    bq.create_table(table, exists_ok=True)
    log_info(f"RAW_TABLE_READY table={table_ref}")


# -------------------- GCS --------------------


def list_gcs_blobs(gcs, bucket_name, prefix):
    """List all CSV blob URIs under the given prefix."""
    bucket = gcs.bucket(bucket_name)
    blobs = [
        f"gs://{bucket_name}/{b.name}"
        for b in bucket.list_blobs(prefix=prefix)
        if b.name.endswith(".csv")
    ]
    log_info(f"GCS_BLOBS_FOUND count={len(blobs)} prefix={prefix}")
    return blobs


# -------------------- LOAD --------------------


def load_uri(bq, dataset, gcs_uri):
    """Run a BigQuery load job for a single GCS URI."""
    table_ref = f"{bq.project}.{dataset}.{RAW_TABLE}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="started_at",
        ),
    )

    log_info(f"LOAD_START uri={gcs_uri}")

    def _load():
        job = bq.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        job.result()  # wait for completion
        if job.errors:
            raise RuntimeError(f"Load job errors: {job.errors}")
        log_info(f"LOAD_DONE uri={gcs_uri} rows={job.output_rows}")

    retry(_load)


# -------------------- MAIN PIPELINE --------------------


def load(bucket, dataset, prefix):
    """End-to-end load pipeline."""
    log_info("LOAD_PIPELINE_START")

    bq = get_bq_client()
    gcs = get_gcs_client()

    ensure_meta_table(bq, dataset)
    ensure_raw_table(bq, dataset)

    already_loaded = load_loaded_uris(bq, dataset)
    all_uris = list_gcs_blobs(gcs, bucket, prefix)

    pending = [uri for uri in all_uris if uri not in already_loaded]
    log_info(f"PENDING count={len(pending)}")

    for uri in pending:
        try:
            load_uri(bq, dataset, uri)
            mark_loaded(bq, dataset, uri)
        except Exception as e:
            log_error(f"LOAD_FAIL uri={uri} error={e}")

    log_info("LOAD_PIPELINE_DONE")


# -------------------- CLI ENTRY --------------------


def main():
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        raise RuntimeError("Missing GOOGLE_APPLICATION_CREDENTIALS")

    parser = argparse.ArgumentParser(
        description="Load Citibike CSVs from GCS into BigQuery"
    )
    parser.add_argument(
        "--year", default="2024", choices=["2024", "2025"], help="Year of trip data"
    )
    parser.add_argument("--month", default="1", help="Month of trip data")
    parser.add_argument(
        "--bucket", default="de_citibike_bucket", help="GCS bucket name"
    )
    parser.add_argument(
        "--dataset", default="de_citibike_raw", help="BigQuery dataset name"
    )
    args = parser.parse_args()

    month = args.month.zfill(2)
    prefix = f"csv/{args.year}/{args.year}{month}-citibike-tripdata"

    load(
        bucket=args.bucket,
        dataset=args.dataset,
        prefix=prefix,
    )


if __name__ == "__main__":
    main()
