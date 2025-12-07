FROM apache/airflow:3.0.2-python3.12

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy include modules for custom Python code
COPY include/ ./include/

# Add Airflow directory to Python path for include module imports
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Optimize Airflow for high parallelism (40+ partition tasks for large datasets)
ENV AIRFLOW__CORE__PARALLELISM=128
ENV AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=64

# Scheduler optimizations for faster task pickup
ENV AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=10
ENV AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=5
ENV AIRFLOW__SCHEDULER__NUM_RUNS=-1

# Database connection pool for parallel tasks
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=10
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=20
