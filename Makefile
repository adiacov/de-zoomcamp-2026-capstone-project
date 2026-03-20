ingest:
# 	this function expects arguments, check the implementation. currently using defaults.
	@uv run -m src.ingest.citibike_ingest

load:
# 	this function expects arguments, check the implementation. currently using defaults.
	@uv run -m src.load.citibike_load
