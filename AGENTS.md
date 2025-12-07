# Repository Guidelines

## Project Structure & Module Organization
- DAGs live in `dags/` (`postgres_to_postgres_migration.py`, `validate_migration_env.py`); docs in `docs/`; shared migration logic in `include/mssql_pg_migration/` (schema extraction, DDL, data transfer, validation helpers); tests in `tests/dags/`; infra/config in `docker-compose.yml`, `Dockerfile`, `.env*`, and `postgres_config/`.
- Data samples go under `data/`; Airflow logs under `logs/`; helper scripts in `scripts/` and `integration_test.sh`.

## Build, Test, and Development Commands
- Start stack: `docker-compose up -d` (brings up source/target Postgres and Airflow). Recreate after env changes: `docker-compose up -d --force-recreate`.
- Trigger migration DAG: `docker exec airflow-scheduler airflow dags trigger postgres_to_postgres_migration`.
- Trigger validation DAG: `docker exec airflow-scheduler airflow dags trigger validate_migration_env`.
- Run tests: `docker exec airflow-scheduler pytest tests/` (initialize Airflow DB first if missing: `docker exec airflow-scheduler airflow db init`).
- Tail logs: `docker-compose logs -f` or service-specific `docker-compose logs -f airflow-scheduler`.

## Coding Style & Naming Conventions
- Python, 4-space indentation, PEP8-friendly. Keep functions small and composable; prefer explicit over magic.
- DAG/task IDs: lowercase with underscores; params/conn IDs should align with `.env` (e.g., `SOURCE_CONN_ID`, `TARGET_CONN_ID`).
- Avoid new heavy dependencies; the stack standardizes on `psycopg2` for Postgres connectivity.

## Testing Guidelines
- Use `pytest` under `tests/`; add DAG import/validation tests alongside new DAGs/operators.
- Name tests `test_*.py` and functions `test_*`. Prefer deterministic, container-friendly tests (no external network).
- For data-path changes, document manual validation steps (row-count checks, partition behavior) in docs or PR notes.

## Commit & Pull Request Guidelines
- Commit messages are concise, imperative summaries (e.g., “Standardize on psycopg2, remove pg8000 dependency”).
- PRs should describe intent, key changes, validation (commands/logs), and any config/env impacts. Link issues/tickets when applicable.
- Include screenshots only when UI/log output materially helps reviewers (otherwise, summarize).

## Security & Configuration Tips
- Centralize config in `.env` (see `.env.example`); keep secrets out of commits.
- Use the provided connection IDs (`postgres_source`, `postgres_target`) unless there’s a documented override; keep Airflow and database credentials in environment, not code.
- Large datasets belong outside the image build context (`data/`, ensure `.dockerignore` excludes bulky dumps).
