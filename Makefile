.DEFAULT_GOAL := help

UV ?= uv
COUNT ?= 10
SEED ?= 42
SAMPLE_COUNT ?= 5000
SAMPLE_SEED ?= 2026
SAMPLES_DIR ?= data/samples
PYTEST_ARGS ?=

.PHONY: help install sync update run ride order samples demo lint format fix check test hooks pre-commit clean distclean

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_-]+:.*## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: sync ## Alias for sync

sync: ## Create or refresh the virtualenv from the lockfile
	$(UV) sync

update: ## Upgrade locked dependencies and sync
	$(UV) lock --upgrade
	$(UV) sync

run: ## Run the package entry point
	$(UV) run pipeline-healer

ride: ## Print generated ride events (COUNT=, SEED=)
	$(UV) run ride-event-producer --count $(COUNT) --seed $(SEED)

order: ## Print generated order events (COUNT=, SEED=)
	$(UV) run order-event-producer --count $(COUNT) --seed $(SEED)

samples: ## Regenerate the committed sample datasets
	$(UV) run generate-samples --out $(SAMPLES_DIR) --count $(SAMPLE_COUNT) --seed $(SAMPLE_SEED)

demo: ## Show a few events from both streams
	@echo "== ride events =="
	@$(UV) run ride-event-producer --count 3 --seed $(SEED)
	@echo "== order events =="
	@$(UV) run order-event-producer --count 3 --seed $(SEED)

lint: ## Lint with Ruff
	$(UV) run ruff check .

format: ## Format with Ruff
	$(UV) run ruff format .

fix: ## Apply Ruff fixes, then format
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

check: ## Run lint, formatting check, and tests
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run pytest $(PYTEST_ARGS)

test: ## Run the test suite (PYTEST_ARGS= for options)
	$(UV) run pytest $(PYTEST_ARGS)

hooks: ## Install the pre-commit git hook
	$(UV) run pre-commit install

pre-commit: ## Run all hooks against every file
	$(UV) run pre-commit run --all-files

clean: ## Remove caches and build artifacts (keeps .venv)
	rm -rf .pytest_cache .ruff_cache .mypy_cache .hypothesis dist build *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -not -path './.venv/*' -prune -exec rm -rf {} +

distclean: clean ## Remove caches and the virtualenv
	rm -rf .venv
