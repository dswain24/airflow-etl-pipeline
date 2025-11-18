README.md
requirements.txt
dags/
etl_example_dag.py
scripts/
transform_data.py
data/
input.csv

```
README.md (COPY/PASTE)
```markdown
# Airflow ETL Pipeline (Dummy CSV to Transformed Output)
## Overview
This project demonstrates a simple ETL pipeline orchestrated by Apache Airflow. It uses a dummy CSV file,
applies
a basic transformation using Python and pandas, and simulates loading the transformed data. The focus is
on ETL
orchestration, task dependencies, and re-runnability.
## Tech Stack
- Apache Airflow
- Python
- pandas
- Dummy CSV data
## How It Works
1. The `extract` task simulates pulling data (in a real system this might be from S3 or a database).
2. The `transform` task uses a Python script to clean and enrich the data.
3. The `load` task simulates loading into a warehouse (e.g., Redshift or Snowflake).
## Prerequisites
- Docker Desktop running (for the official Airflow Docker Compose setup).
- Python (if you want to run pieces locally).
## Airflow Folder Mapping (Docker-based setup)
In the root of your repo (where `docker-compose.yaml` from the Airflow docs lives):
- `./dags` → place `etl_example_dag.py` here.
- `./dags/scripts` → place `transform_data.py` here.
- `./dags/data` → place `input.csv` here.
## How to Run with Airflow + Docker
1. Download the official Airflow `docker-compose.yaml` to the repo root (outside this project folder).
2. Create required folders:
```bash
mkdir -p ./dags ./logs ./plugins
```
3. Copy your DAG and scripts:
- Copy `airflow-etl-pipeline/dags/etl_example_dag.py` → `./dags/`
- Copy `airflow-etl-pipeline/scripts` → `./dags/scripts/`
- Copy `airflow-etl-pipeline/data/input.csv` → `./dags/data/input.csv`
4. Initialize Airflow:
```bash
docker-compose up airflow-init
```
5. Start all Airflow services:
```bash
docker-compose up -d
```
6. Open the Airflow UI:
- Go to `http://localhost:8080` in your browser.
- Login (default: `airflow` / `airflow` if unchanged).
- Turn on `simple_etl_example` DAG and trigger it.
## Common Errors and Debugging Tips
**Error 1: Airflow web UI not reachable**
- Cause: Containers may not be running.
- Fix:
- Run `docker ps` and verify Airflow containers are up.
- If not, run `docker-compose up -d` from the directory containing `docker-compose.yaml`.

**Error 2: `ModuleNotFoundError: No module named 'pandas'` inside Airflow**
- Cause: Airflow image may not include extra packages.
- Fix: Either:
- Use the `requirements.txt` with an Airflow custom image, or
- Mount a requirements file and follow Airflow docs for installing python deps in the container.
**Error 3: DAG not appearing in the UI**
- Causes:
- Wrong folder mapping (DAG not in the `./dags` folder used by Docker).
- Syntax error in the DAG file.
- Fix:
- Check Airflow logs: `docker-compose logs scheduler` and `docker-compose logs webserver`.
- Validate Python code syntax in `etl_example_dag.py`.
```
requirements.txt
```
apache-airflow==2.9.0
pandas==2.2.0
boto3==1.34.0
```
scripts/transform_data.py
```python
import pandas as pd
import sys
from pathlib import Path
def transform(input_path: str, output_path: str):
df = pd.read_csv(input_path)
# Simple dummy transformation
if 'amount' in df.columns:
df['amount'] = df['amount'].fillna(0)
df['amount_double'] = df['amount'] * 2
Path(output_path).parent.mkdir(parents=True, exist_ok=True)
df.to_csv(output_path, index=False)
if __name__ == '__main__':
input_file = sys.argv[1]
output_file = sys.argv[2]
transform(input_file, output_file)
```
dags/etl_example_dag.py
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
default_args = {
'owner': 'tijandre',
'depends_on_past': False,
'retries': 1,
'retry_delay': timedelta(minutes=5),
}
with DAG(
dag_id='simple_etl_example',
default_args=default_args,
schedule_interval='@daily',
start_date=datetime(2025, 1, 1),
catchup=False,

tags=['etl', 'example'],
) as dag:
extract = BashOperator(
task_id='extract',
bash_command='echo "Simulating extract step"',
)
transform = BashOperator(
task_id='transform',
bash_command=('python /opt/airflow/dags/scripts/transform_data.py '
' /opt/airflow/dags/data/input.csv '
' /opt/airflow/dags/data/output.csv'),
)
load = BashOperator(
task_id='load',
bash_command='echo "Simulating load step"',
)
extract >> transform >> load
