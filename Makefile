.DEFAULT_GOAL := help

.PHONY: help test pylint install-dev

help: ## Show this help.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*##";} /^[a-zA-Z0-9_\\-]+:.*##/ {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install-dev: ## Install project in editable mode with dev deps.
	@python -m pip install -e ".[dev]"

test: ## Run pytest.
	@python -m pytest -v

pylint: ## Run pylint on the mzb package.
	@python -m pylint mzb tests
