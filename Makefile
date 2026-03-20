ingest:
	@uv run -m src.ingest.citibike_ingest --year 2024 --month 1 --bucket de_citibike_bucket

load:
	@uv run -m src.load.citibike_load --year 2024 --month 1 --bucket de_citibike_bucket --dataset de_citibike_dataset
