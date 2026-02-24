.PHONY: help install setup download seed run test docs clean docker-build docker-up validate export-pii

# ============================================
# Variables
# ============================================
PYTHON := uv run python
DBT := cd dbt_project && uv run dbt
DOCKER_COMPOSE := docker-compose

# Colors
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

##@ General

help: ## Display this help message
	@echo "$(BLUE) Dbt RGPD Anonymizer - Available Commands$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make $(YELLOW)<target>$(NC)\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(BLUE)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Installation

install: ## Install Python dependencies using uv
	@echo "$(BLUE) Installing dependencies...$(NC)"
	uv sync
	@echo "$(GREEN) Installation completed$(NC)"

setup: install ## Full project setup
	@echo "$(BLUE) Setting up project...$(NC)"
	cp -n .env.example .env || true
	@echo "$(YELLOW) Remember to configure your .env file!$(NC)"
	mkdir -p dbt_project/logs
	$(DBT) deps
	@echo "$(GREEN) Setup completed$(NC)"

##@ Data

download: ## Download full dataset from data.gouv.fr
	@echo "$(BLUE) Downloading dataset...$(NC)"
	$(PYTHON) scripts/download_data.py
	@echo "$(GREEN) Download completed$(NC)"

download-sample: ## Download a 100-row sample dataset
	@echo "$(BLUE) Downloading sample dataset...$(NC)"
	$(PYTHON) scripts/download_data.py --sample 100
	@echo "$(GREEN) Sample downloaded$(NC)"

##@ dbt

seed: ## Load seeds into DuckDB
	@echo "$(BLUE) Loading seeds...$(NC)"
	$(DBT) seed
	@echo "$(GREEN) Seeds loaded$(NC)"

run: ## Run all dbt models
	@echo "$(BLUE) Running dbt models...$(NC)"
	$(DBT) run
	@echo "$(GREEN) Models executed$(NC)"

run-staging: ## Run only staging models
	@echo "$(BLUE) Running staging models...$(NC)"
	$(DBT) run --select staging
	@echo "$(GREEN) Staging completed$(NC)"

run-marts: ## Run only marts models
	@echo "$(BLUE) Running marts...$(NC)"
	$(DBT) run --select marts
	@echo "$(GREEN) Marts completed$(NC)"

test: ## Run all dbt tests
	@echo "$(BLUE) Running dbt tests...$(NC)"
	$(DBT) test
	@echo "$(GREEN) Tests passed$(NC)"

docs: ## Generate and serve dbt documentation
	@echo "$(BLUE) Generating documentation...$(NC)"
	$(DBT) docs generate
	@echo "$(GREEN) Documentation generated$(NC)"
	@echo "$(BLUE) Starting documentation server...$(NC)"
	$(DBT) docs serve

docs-generate: ## Generate documentation without serving
	@echo "$(BLUE) Generating documentation...$(NC)"
	$(DBT) docs generate
	@echo "$(GREEN) Documentation available in dbt_project/target/$(NC)"

##@ Full Pipeline

full-pipeline: download seed run test ## Full pipeline: download → seed → run → test
	@echo "$(GREEN) Full pipeline completed successfully$(NC)"

quick-pipeline: seed run test ## Quick pipeline: seed → run → test
	@echo "$(GREEN) Quick pipeline completed$(NC)"

##@ GDPR Validation

validate: ## Validate anonymization rules
	@echo "$(BLUE) Validating anonymization...$(NC)"
	$(PYTHON) scripts/validate_anonymization.py
	@echo "$(GREEN) Validation completed$(NC)"

export-pii: ## Export PII report as JSON
	@echo "$(BLUE) Exporting PII report (JSON)...$(NC)"
	$(PYTHON) scripts/export_pii_report.py
	@echo "$(GREEN) Report saved to dbt_project/logs/pii_report.json$(NC)"

export-pii-csv: ## Export PII report as CSV
	@echo "$(BLUE) Exporting PII report (CSV)...$(NC)"
	$(PYTHON) scripts/export_pii_report.py --format csv
	@echo "$(GREEN) CSV report exported$(NC)"

audit: test validate export-pii ## Full audit: tests + validation + PII export
	@echo "$(GREEN) Full audit completed$(NC)"

##@ Docker

docker-build: ## Build Docker image
	@echo "$(BLUE) Building Docker image...$(NC)"
	$(DOCKER_COMPOSE) build
	@echo "$(GREEN) Image built$(NC)"

docker-up: ## Start Docker containers
	@echo "$(BLUE) Starting containers...$(NC)"
	$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN) Containers started$(NC)"

docker-down: ## Stop Docker containers
	@echo "$(BLUE) Stopping containers...$(NC)"
	$(DOCKER_COMPOSE) down
	@echo "$(GREEN) Containers stopped$(NC)"

docker-logs: ## Show dbt container logs
	$(DOCKER_COMPOSE) logs -f dbt

docker-shell: ## Open a shell inside the dbt container
	$(DOCKER_COMPOSE) exec dbt /bin/bash

docker-run-pipeline: ## Run the full pipeline inside Docker
	@echo "$(BLUE) Running pipeline inside Docker...$(NC)"
	$(DOCKER_COMPOSE) exec dbt dbt seed
	$(DOCKER_COMPOSE) exec dbt dbt run
	$(DOCKER_COMPOSE) exec dbt dbt test
	@echo "$(GREEN) Docker pipeline completed$(NC)"

##@ Cleanup

clean: ## Clean temporary files and logs
	@echo "$(BLUE) Cleaning project...$(NC)"
	rm -rf dbt_project/target/
	rm -rf dbt_project/dbt_packages/
	rm -rf dbt_project/logs/*.log
	rm -f dbt_project/*.duckdb*
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "$(GREEN) Cleanup completed$(NC)"

clean-all: clean ## Full cleanup including data
	@echo "$(BLUE) Performing full cleanup...$(NC)"
	rm -rf dbt_project/seeds/services_publics_raw.csv
	rm -rf .venv/
	@echo "$(GREEN) Full cleanup completed$(NC)"

##@ Development

format: ## Format Python code using ruff
	@echo "$(BLUE) Formatting code...$(NC)"
	uv run ruff format src/dbt_gdpr_anonymizer/scripts/
	@echo "$(GREEN) Code formatted$(NC)"

lint: ## Lint Python code using ruff
	@echo "$(BLUE) Running linter...$(NC)"
	uv run ruff check src/dbt_gdpr_anonymizer/scripts/
	@echo "$(GREEN) Linting completed$(NC)"
