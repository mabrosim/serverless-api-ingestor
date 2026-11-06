.DEFAULT_GOAL := help
SHELL := /bin/bash
VENV := .venv
export PATH := $(abspath $(VENV))/bin:$(PATH)
PYTHON := $(VENV)/bin/python
CONFIG ?= config.yaml

.PHONY: help bootstrap format lint test validate template build check deploy smoke-test logs delete clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-16s %s\n", $$1, $$2}'

bootstrap: ## Sync locked development dependencies with uv
	uv sync --locked --extra dev --python 3.13
	$(PYTHON) -m ensurepip

format: ## Apply formatting and safe fixes
	$(VENV)/bin/ruff check --fix .
	$(VENV)/bin/ruff format .

lint: ## Run source checks
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .

test: ## Run tests
	$(VENV)/bin/pytest

validate: ## Check local configuration readiness, template, and build (no live calls)
	$(PYTHON) scripts/config.py validate $(CONFIG)
	$(MAKE) template build

template: ## Validate provider-neutral SAM/CloudFormation structure
	sam validate --lint --template-file template.yaml
	$(VENV)/bin/cfn-lint template.yaml

build: ## Build SAM application
	sam build
	find .aws-sam/build/FetcherFunction -type f -name .DS_Store -delete
	$(PYTHON) scripts/config.py package-check $(CONFIG)

check: lint test template build ## Check scaffold health independently of deployment readiness

deploy: ## Recheck readiness, then deploy with manual changeset confirmation
	$(MAKE) validate CONFIG=config.yaml
	$(PYTHON) scripts/config.py deploy config.yaml

smoke-test: ## Invoke deployed function
	$(PYTHON) scripts/config.py smoke-test config.yaml

logs: ## Tail Lambda logs
	$(PYTHON) scripts/config.py logs config.yaml

delete: ## Delete deployed stack after confirmation
	$(PYTHON) scripts/config.py delete config.yaml

clean: ## Remove generated artifacts
	rm -rf .aws-sam .pytest_cache .ruff_cache .coverage htmlcov build dist *.egg-info
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
