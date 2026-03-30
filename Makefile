# ──────────────────────────────────────────────
#  Docker
# ──────────────────────────────────────────────

build:
	docker compose build

up:
	docker compose up

up-d:
	docker compose up -d

down:
	docker compose down

# ──────────────────────────────────────────────
#  Terraform
# ──────────────────────────────────────────────

tf-init:
	cd terraform && terraform init

tf-apply:
	cd terraform && terraform apply

tf-destroy:
	cd terraform && terraform destroy

# ──────────────────────────────────────────────
#  Local dev (runs via uv, no Docker required)
# ──────────────────────────────────────────────

ingest:
#	this function expects arguments, check the implementation. currently using defaults.
	@uv run -m citibike.ingest.citibike_ingest --year 2024 --month 1 --bucket de_citibike_bucket

load:
#	this function expects arguments, check the implementation. currently using defaults.
	@uv run -m citibike.load.citibike_load --year 2024 --month 1 --bucket de_citibike_bucket --dataset de_citibike_raw

streamlit-run:
	@streamlit run dashboard/app.py