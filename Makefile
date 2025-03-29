# THINGS THIS SHOULD DO
# 1. set up venv
# 2. install requirements.txt
# 3. set up db
# 4. seed db

WD=$(shell pwd)
TEST_ENV_FILE = .env.test
DEV_ENV_FILE = .env.development

# Load environment variables from .env file
include .env.test
export $(shell sed 's/=.*//' .env.test)

all: install-requirements setup-db

create-environment:
	@echo ">>> Setting up python venv as 'venv'"
	python -m venv venv;
	@echo ">>> Activating venv and setting working directory"
	. venv/bin/activate && export PYTHONPATH=$(WD)

define execute_in_env
	(. venv/bin/activate) && $1
endef

install-requirements: create-environment
	@echo ">>> Installing requirements and setting up pytest"
	$(call execute_in_env, pip install -r ./requirements.txt && pip install pytest && export PYTHONPATH=$(WD) pytest)

setup-db:
	@echo ">>> Setting up seed database"
	$(call execute_in_env, PGPASSWORD=$(PG_PASSWORD) psql -h $(PG_HOST) -U $(PG_USER) -d postgres -p $(PG_PORT) -f db_setup/setup_dbs.sql)