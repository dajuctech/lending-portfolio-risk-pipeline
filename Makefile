.PHONY: help infra ingest dbt-run dbt-test dbt-docs stream spark teardown

help:
	@echo "=============================================="
	@echo "  Lending Portfolio Risk Intelligence Platform"
	@echo "=============================================="
	@echo ""
	@echo "  make infra      Provision GCP resources with Terraform"
	@echo "  make ingest     Start Kestra orchestration stack"
	@echo "  make dbt-run    Build all dbt models"
	@echo "  make dbt-test   Run all dbt tests"
	@echo "  make dbt-docs   Generate + serve dbt documentation"
	@echo "  make stream     Start Redpanda + Flink streaming stack"
	@echo "  make spark      Run PySpark batch analysis"
	@echo "  make teardown   Stop all Docker stacks"
	@echo ""

infra:
	cd terraform && terraform init && terraform apply -auto-approve

ingest:
	cd kestra && docker compose up -d
	@echo "Kestra UI: http://localhost:8080"

dbt-run:
	cd dbt/lending_club && uv run dbt deps && uv run dbt run --profiles-dir .

dbt-test:
	cd dbt/lending_club && uv run dbt test --profiles-dir .

dbt-docs:
	cd dbt/lending_club && uv run dbt docs generate && uv run dbt docs serve --profiles-dir .

stream:
	cd streaming && docker compose up -d --build
	@echo "Flink UI: http://localhost:8082"

spark:
	@echo "Setting Java 21 (required for Spark)..."
	export JAVA_HOME=/usr/local/sdkman/candidates/java/21.0.9-ms && \
	export PATH=$$JAVA_HOME/bin:$$PATH && \
	uv run spark/credit_risk_analysis.py

teardown:
	cd kestra && docker compose down -v || true
	cd streaming && docker compose down -v || true
	@echo "All Docker stacks stopped."
