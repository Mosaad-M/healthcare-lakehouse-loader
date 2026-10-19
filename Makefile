# Doris runs by default; COMPOSE_PROFILES= (empty) leaves it out (the fast CI job).
export COMPOSE_PROFILES ?= doris
COMPOSE := docker compose -f stack/compose.yaml --env-file .env
TOOLBOX := $(COMPOSE) run --rm --no-deps toolbox
UP_TIMEOUT ?= 300

.PHONY: up down clean test verify build demo env clean-test-data scan

# Creates .env, or adds values and secrets it is missing (never overwrites).
env:
	@python3 scripts/make_env.py

build: env
	$(COMPOSE) build

up: env build
	@date +%s > .up-started
	$(COMPOSE) up -d
	python3 scripts/wait_healthy.py $(UP_TIMEOUT) .up-started -- $(COMPOSE)

down:
	$(COMPOSE) --profile '*' down

clean:
	$(COMPOSE) --profile '*' down -v

test: env
	$(TOOLBOX) pytest $(ARGS)

# Security scanners (scripts/scan.sh): dependencies, secrets, SAST, IaC and images.
scan: env
	./scripts/scan.sh

# Every suite. Expects `make up` to have run.
verify: env
	$(TOOLBOX) pytest -q $(ARGS)

# Remove tables, principals and raw files left by interrupted test runs (dry run unless ARGS=--yes).
clean-test-data: env
	$(TOOLBOX) python scripts/clean_test_data.py $(ARGS)

# Drop synthetic files for the example sources and load them through their Prefect deployments.
demo: env
	$(TOOLBOX) python scripts/demo.py

# ---- AWS sandbox. Synthetic data only; the sandbox account only (see infra/sandbox).
SANDBOX := uv run --no-project --with boto3 python scripts/sandbox.py
.PHONY: sandbox-plan sandbox-apply sandbox-test sandbox-destroy

sandbox-plan:
	$(SANDBOX) plan

sandbox-apply:
	$(SANDBOX) apply

sandbox-test:
	$(SANDBOX) test $(ARGS)

sandbox-destroy:
	$(SANDBOX) destroy
