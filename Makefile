.PHONY: up seed etl report test psql down build

build:
	docker compose build

up:
	docker compose up -d localstack postgres

seed:
	docker compose run --rm spark spark-submit jobs/generate_raw_data.py

etl:
	docker compose run --rm spark spark-submit jobs/raw_to_staging.py
	docker compose run --rm spark spark-submit jobs/staging_to_warehouse.py

report:
	docker compose run --rm spark spark-submit jobs/regulatory_reports.py

test:
	docker compose run --rm tests

psql:
	docker compose exec postgres psql -U $${POSTGRES_USER:-warehouse} -d $${POSTGRES_DB:-warehouse}

down:
	docker compose down -v
