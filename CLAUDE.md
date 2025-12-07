# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Apache Airflow project using vanilla Apache Airflow 2.10.4 with Docker Compose for orchestrating data pipelines. The project is designed for ETL processes from PostgreSQL source to PostgreSQL target.

## Common Development Commands

### Starting and Managing Airflow

```bash
# Start all services (databases + Airflow)
docker-compose up -d

# Stop all services
docker-compose down

# Restart services
docker-compose restart

# View logs
docker-compose logs -f

# Rebuild after code changes
docker-compose down && docker-compose build && docker-compose up -d
```

### Running Tests

```bash
# Check for DAG import errors
docker exec airflow-scheduler airflow dags list-import-errors

# Run pytest tests
docker exec airflow-scheduler pytest tests/

# List all DAGs
docker exec airflow-scheduler airflow dags list
```

### Triggering DAGs

```bash
# Trigger migration DAG
docker exec airflow-scheduler airflow dags trigger postgres_to_postgres_migration

# Trigger with configuration
docker exec airflow-scheduler airflow dags trigger postgres_to_postgres_migration \
  --conf '{"drop_existing_tables": true}'

# Check DAG run status
docker exec airflow-scheduler airflow dags list-runs -d postgres_to_postgres_migration
```

## Architecture and Code Structure

### DAG Development Pattern

This project uses Airflow's TaskFlow API (@task decorator) for writing DAGs. Key patterns:

1. **DAG Definition**: Use the @dag decorator with proper configuration:
```python
from airflow.sdk import Asset, dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 4, 22),
    schedule="@daily",
    default_args={"owner": "data-team", "retries": 3},
    tags=["example"],  # Required by tests
)
def my_dag():
    # DAG logic here
```

2. **Task Creation**: Use @task decorator for Python functions:
```python
@task
def extract_data(**context) -> dict:
    # Task implementation
    return data
```

3. **Dynamic Task Mapping**: For parallel processing of variable-sized data:
```python
process_task.partial(static_param="value").expand(
    dynamic_param=get_data_list()
)
```

4. **Data Assets**: Define outlets for downstream dependencies:
```python
@task(outlets=[Asset("table_name")])
def update_table():
    # Task that updates a data asset
```

### Testing Requirements

All DAGs must meet these criteria (enforced by tests/dags/test_dag_example.py):

1. **Tags Required**: Every DAG must have at least one tag
2. **Retry Policy**: default_args must include `retries >= 2`
3. **Import Validation**: DAG files must import without errors
4. **Naming Convention**: Place all DAG files in the `dags/` directory

### Docker and Deployment

The project uses Apache Airflow 2.10.4 (Dockerfile). When modifying dependencies:

1. **Python packages**: Add to `requirements.txt`
2. **Environment variables**: Use `.env` file
3. **Connections/Variables**: Configure in `.env` file using `AIRFLOW_CONN_*` and `AIRFLOW_VAR_*` environment variables

### Local Development Environment

When `docker-compose up -d` is running:
- Airflow UI: http://localhost:8080/ (user: admin, password: admin)
- PostgreSQL Source: localhost:5434/source_db (user: postgres, pass: PostgresPassword123)
- PostgreSQL Target: localhost:5435/target_db (user: postgres, pass: PostgresPassword123)
- Airflow metadata DB: internal (airflow-postgres container)

Containers running:
- postgres-source
- postgres-target
- airflow-postgres
- airflow-webserver
- airflow-scheduler
- airflow-triggerer

### Connection and Variable Management

Connections and variables are defined in `.env` file using environment variables:
```bash
# Airflow connections as environment variables
# Format: AIRFLOW_CONN_{CONN_ID}='{conn_type}://{login}:{password}@{host}:{port}/{schema}'

AIRFLOW_CONN_POSTGRES_SOURCE='postgresql://postgres:PostgresPassword123@postgres-source:5432/source_db'
AIRFLOW_CONN_POSTGRES_TARGET='postgresql://postgres:PostgresPassword123@postgres-target:5432/target_db'

# Airflow variables
AIRFLOW_VAR_TARGET_SCHEMA='public'
```

### DAG File Structure

Place DAGs in `/dags` directory. Use this structure:
```python
"""
Docstring explaining DAG purpose and workflow
"""

from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    default_args={"owner": "data-team", "retries": 3},
    tags=["etl", "postgres"],
)
def pipeline_name():
    @task
    def extract():
        # Extract logic
        pass

    @task
    def transform(data):
        # Transform logic
        pass

    @task
    def load(data):
        # Load logic
        pass

    # Define task dependencies
    load(transform(extract()))

# Instantiate DAG
pipeline_name()
```

### Project-Specific Considerations

1. **PostgreSQL to PostgreSQL Migration**: When implementing ETL pipelines:
   - Use the apache-airflow-providers-postgres provider package
   - Both source and target use the same PostgreSQL data types
   - Handle schema differences and naming conventions

2. **Error Handling**: Tasks should include proper exception handling and use Airflow's retry mechanism

3. **XCom Usage**: For passing data between tasks, use return values or context["ti"].xcom_push()

4. **Testing DAGs**: Always validate new DAGs with `docker exec airflow-scheduler airflow dags list-import-errors` before committing
