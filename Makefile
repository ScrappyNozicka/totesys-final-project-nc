#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = Totesys-Final-Project
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}:${WD}/src/extract:${WD}/src/transform:${WD}/src/transform/transform_utils:${WD}/src/load
SHELL := /bin/bash
PROFILE = default
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

################################################################################################################
# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install pytest-cov)

## Install pip-audit
pip-audit:
	$(call execute_in_env, $(PIP) install pip-audit)	

## Set up dev requirements (bandit, black & coverage)
dev-setup: bandit black coverage pip-audit

# Build / Run

## Run the security test (bandit)
security-test:
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)

## Run the black code check
run-black:
	$(call execute_in_env, black --line-length 79 ./src/*/*/*.py ./test/*/*.py)

## Run flake8 code check
run-flake8:
	$(call execute_in_env, flake8  ./src/*/*/*.py ./test/*/*.py)	

## Run the unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --ignore=terraform/ -vvvrP)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --ignore=terraform/ --cov=src --cov-report=term-missing --cov-report=html)

## Run pip audit
run-pip-audit:
	$(call execute_in_env, pip-audit)


## Run all checks
run-checks: security-test run-black unit-test check-coverage run-flake8 run-pip-audit