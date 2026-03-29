ingest:
# 	this function expects arguments, check the implementation. currently using defaults.
	@uv run -m citibike.ingest.citibike_ingest

load:
# 	this function expects arguments, check the implementation. currently using defaults.
	@uv run -m citibike.load.citibike_load --month 2024 --month 1 --bucket de_citibike_bucket --dataset de_citibike_raw

streamlit-run:
	@streamlit run streamlit/app.py
