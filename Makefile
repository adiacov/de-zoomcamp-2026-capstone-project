ingest:
	@uv run python -m citibike.ingest.citibike_ingest --year 2024 --month 1 --bucket de_citibike_bucket

load:
	@uv run python -m citibike.load.citibike_load --year 2024 --month 1 --bucket de_citibike_bucket --dataset de_citibike_raw
