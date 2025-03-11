# ToteSys Final Project

## Description
Data platform for extracting, transforming and loading data from ToteSys(Postgres Database) into a Data Warehouse hosted on AWS. The system runs on a 30-minute schedule:
1. First Lambda extracts data from ToteSys and stores it into Ingestion S3
2. Second Lambda takes latest data from Ingestion S3, transforms it into star-schema and stores it in Processed S3
3. Third Lambda takes processed data and loads it into Data Warehouse


## Technologies used
- **Python:** Version 3.12
- **AWS:** S3, Lambda, CloudWatch, EventBridge, Step Functions
- **Pandas:** For data transformation
- **Data Formats:** JSON, Parquet for storing data
- **Automation Tools:**  CI/CD for deployment, Infrastructure-as-Code - Terraform

## Installation

### Prerequisites
- [Python](https://www.python.org/downloads/) - version 3.12 or above
- [Make](https://www.gnu.org/software/make/)
- [Terraform](https://www.terraform.io/downloads.html)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)

### AWS requirements
- create a bucket for tfstate(default name: `kettslough-tfstatebucket`)
- create a secret with Postgres credentials(default name: `ketts-lough-secrets`), required secret keys:
    - DB_USER
    - DB_PASSWORD
    - DB_HOST
    - DB_NAME
    - DB_PORT
    - DW_USER
    - DW_PASSWORD
    - DW_HOST
    - DW_NAME
    - DW_PORT

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Miigget/totesys-final-project.git
   cd totesys-final-project
2. Install dependencies:
    - `create-environment`(automated by `requirements`): Creates a Python virtual environment.
        ```bash
        make create-environment
    - `requirements`: Installs the project dependencies from requirements.txt.
        ```bash
        make requirements
    - `dev-setup`: Installs development tools(bandit, black, pytest-cov, and pip-audit)
        ```bash
        make dev-setup
    - `run-checks`: Runs security tests, code checks, unit tests, coverage analysis
        ```bash
        make run-checks
3. Set up AWS credentials:
    ```bash
    aws configure
4. Set up the AWS infrastructure:
    ```bash
    cd terraform/etl_tf
    terraform init
    terraform apply
