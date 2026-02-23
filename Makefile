# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Byron Marohn
.DEFAULT_GOAL := help

.PHONY: help test pylint

help: ## Show this help.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*##";} /^[a-zA-Z0-9_\\-]+:.*##/ {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: ## Run pytest.
	@python3 -m pytest -v

pylint: ## Run pylint on the mzb package.
	@python3 -m pylint mzb tests
