import os
import sys
import json
import time
import random
import tempfile
import requests
from pathlib import Path
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor, as_completed

import argparse
from google.cloud import storage
from tqdm import tqdm
from dotenv import load_dotenv


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


def ensure_environment():
    """Ensures important env vars are set."""

    environment = os.environ.get("ENVIRONMENT")
    if environment == "container":
        # Container environment. Env vars are set in the docker.
        log_info("Fetching GOOGLE_APPLICATION_CREDENTIALS in container environment.")
    else:
        log_info("Fetching GOOGLE_APPLICATION_CREDENTIALS locally.")
        load_dotenv()

    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        raise RuntimeError("Missing GOOGLE_APPLICATION_CREDENTIALS")


def get_client():
    """Init GCS client."""
    return storage.Client()


# -------------------- CACHE --------------------


def _cache_path() -> Path:
    """
    Resolve cache file path.
    Precedence:
      1. CITIBIKE_CACHE env var  (set in container via Docker/Compose)
      2. Project root  (local dev — walks up from this file to find pyproject.toml)
      3. Fallback: next to this source file
    """
    env_path = os.environ.get("CITIBIKE_CACHE")
    if env_path:
        return Path(env_path)

    # Local: anchor to project root (where pyproject.toml lives)
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").exists():
            return parent / ".ingestion_cache.json"

    # Fallback: next to this file
    return here.parent / ".ingestion_cache.json"


def load_cache() -> dict:
    """Load metadata cache from disk."""
    p = _cache_path()
    return json.loads(p.read_text()) if p.exists() else {}


def save_cache(cache: dict) -> None:
    """Persist metadata cache."""
    p = _cache_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(cache, indent=2))


# -------------------- REMOTE METADATA --------------------


def fetch_remote_meta(url):
    """Fetch remote file metadata via HEAD request only."""
    r = requests.head(url)
    r.raise_for_status()
    return {
        "etag": r.headers.get("ETag"),
        "size": int(r.headers.get("content-length", 0)),
    }


# -------------------- GCS METADATA --------------------


def fetch_gcs_meta(bucket, prefix):
    """
    Return a dict of {blob_name: {size, md5}} for all blobs under prefix.
    No data is downloaded — metadata only.
    """
    meta = {}
    for blob in bucket.list_blobs(prefix=prefix):
        meta[blob.name] = {"size": blob.size, "md5": blob.md5_hash}
    log_info(f"GCS_META count={len(meta)}")
    return meta


# -------------------- DOWNLOAD (temp) --------------------


def download_to_temp(url, size):
    """
    Download ZIP to a temporary file.
    Caller is responsible for cleanup.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    log_info(f"DOWNLOAD_START size_mb={size / 1024 / 1024:.1f} tmp={tmp.name}")

    def _download():
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with tqdm(
                total=size,
                unit="B",
                unit_scale=True,
                file=sys.stdout,
            ) as p:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        tmp.write(chunk)
                        p.update(len(chunk))
        tmp.flush()

    retry(_download)
    tmp.close()
    log_info(f"DOWNLOAD_DONE tmp={tmp.name}")
    return tmp.name


# -------------------- GCS UPLOAD --------------------


def upload(bucket, blob_path, file_obj, size):
    """Upload with resumable chunking + retry."""
    blob = bucket.blob(blob_path)
    blob.chunk_size = 5 * 1024 * 1024  # 5 MB chunks

    def _upload():
        with tqdm(
            total=size,
            unit="B",
            unit_scale=True,
            desc=Path(blob_path).name,
            file=sys.stdout,
        ) as p:
            blob.upload_from_file(file_obj, size=size, timeout=600)
            p.update(size)

    log_info(f"UPLOAD_START file={blob_path} size_mb={size / 1024 / 1024:.1f}")
    retry(_upload)
    log_info(f"UPLOAD_DONE file={blob_path}")


# -------------------- ZIP PROCESSING --------------------


def process_zip(zip_path, bucket, prefix, gcs_meta, max_workers=4):
    """
    Extract ZIP entries and upload to GCS in parallel.
    Skips files whose size already matches GCS metadata.
    Thread-safe: each worker reopens the ZIP independently.
    """
    with ZipFile(zip_path) as z:
        files = z.infolist()

    def worker(info):
        blob_path = f"{prefix.rstrip('/')}/{info.filename}"
        gcs_entry = gcs_meta.get(blob_path)

        if gcs_entry and gcs_entry["size"] == info.file_size:
            log_info(f"UPLOAD_SKIP file={blob_path}")
            return

        try:
            with ZipFile(zip_path) as z:
                with z.open(info.filename) as f:
                    upload(bucket, blob_path, f, info.file_size)
        except Exception as e:
            log_error(f"UPLOAD_FAIL file={blob_path} error={e}")

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(worker, f) for f in files]
        for f in as_completed(futures):
            f.result()


# -------------------- MAIN PIPELINE --------------------


def ingest(url, bucket_name, blob_prefix, force=False) -> bool:
    """
    ETag-driven ingestion pipeline.

    Steps:
      1. HEAD the remote ZIP to get ETag (always — one cheap request).
      2. Compare ETag with local cache.
         If match and not --force → skip everything, exit early.
      3. If ETag changed, cache missing, or --force:
         a. List GCS blob metadata (source of truth for per-file skip logic).
         b. Download ZIP to a temp file.
         c. Upload only missing/changed inner files (size-based GCS check).
         d. Update local cache with new ETag.
         e. Delete temp ZIP file.

    Repair mode (--force):
      Bypasses the ETag check. GCS per-file size check still applies,
      so only genuinely missing files are uploaded — no wasted bandwidth.

    Returns:
        True if new data was downloaded, False if skipped
    """
    log_info(f"INGEST_PIPELINE_START: {url}")
    cache = load_cache()

    remote_meta = retry(lambda: fetch_remote_meta(url))
    log_info(
        f"REMOTE_META etag={remote_meta['etag']} size_mb={remote_meta['size'] / 1024 / 1024:.1f}"
    )

    cached = cache.get(url, {})
    etag_match = cached.get("etag") == remote_meta["etag"]

    if etag_match and not force:
        log_info("ETAG_MATCH — nothing changed, skipping pipeline")
        log_info("PIPELINE_DONE")
        return False

    if force:
        log_info("FORCE_MODE — bypassing cache, validating GCS state")

    client = get_client()
    bucket = client.bucket(bucket_name)
    gcs_meta = fetch_gcs_meta(bucket, blob_prefix)

    zip_path = None
    try:
        zip_path = download_to_temp(url, remote_meta["size"])
        process_zip(zip_path, bucket, blob_prefix, gcs_meta)

        cache[url] = {
            "etag": remote_meta["etag"],
            "size": remote_meta["size"],
        }
        save_cache(cache)
        log_info("CACHE_UPDATED")

    finally:
        if zip_path and Path(zip_path).exists():
            Path(zip_path).unlink()
            log_info(f"TEMP_DELETED path={zip_path}")

    log_info("INGEST_PIPELINE_DONE")

    return True


# -------------------- APP --------------------
def run_ingest(
    year: str,
    month: str,
    bucket: str,
    force: bool = False,
):
    ensure_environment()

    month = str(month).zfill(2)
    file_name = f"{str(year)}{month}-citibike-tripdata.zip"
    source_url = f"https://s3.amazonaws.com/tripdata/{file_name}"
    blob_prefix = f"csv/{str(year)}"

    ingest(source_url, bucket, blob_prefix, force)


# -------------------- CLI --------------------


def main():
    ensure_environment()

    parser = argparse.ArgumentParser(description="Ingest Citibike data to GCS")
    parser.add_argument(
        "--year", default="2024", choices=["2024", "2025"], help="Year of trip data"
    )
    parser.add_argument("--month", default="1", help="Month of trip data")
    parser.add_argument(
        "--bucket", default="de_citibike_bucket", help="GCS bucket name"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass ETag cache and re-validate GCS state. Use to repair missing blobs.",
    )
    args = parser.parse_args()

    month = args.month.zfill(2)
    file_name = f"{args.year}{month}-citibike-tripdata.zip"
    source_url = f"https://s3.amazonaws.com/tripdata/{file_name}"
    blob_prefix = f"csv/{args.year}"

    return ingest(source_url, args.bucket, blob_prefix, force=args.force)


if __name__ == "__main__":
    main()
