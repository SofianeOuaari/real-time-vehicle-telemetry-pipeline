"""
Analysis Pipeline DAG

Runs AI-powered analysis on collected telemetry data every 15 minutes.
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from pathlib import Path
from docker.types import Mount

default_args = {
    'owner': 'analytics-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def check_parquet_availability(**context):
    """Check if parquet files exist"""
    parquet_dir = Path('/opt/airflow/data/parquet')
    if parquet_dir.exists() and list(parquet_dir.glob('**/*.parquet')):
        return 'prepare_duckdb'
    return 'skip_analysis'


def prepare_duckdb(**context):
    """Load Parquet files into DuckDB"""
    import duckdb
    db_path = '/opt/airflow/data/telemetry.duckdb'
    parquet_path = '/opt/airflow/data/parquet/**/*.parquet'

    con = duckdb.connect(db_path)
    con.execute(f"CREATE OR REPLACE TABLE telemetry_data AS SELECT * FROM read_parquet('{parquet_path}')")
    count = con.execute("SELECT COUNT(*) FROM telemetry_data").fetchone()[0]
    con.close()

    context['task_instance'].xcom_push(key='record_count', value=count)
    return count


with DAG(
    'analysis_pipeline',
    default_args=default_args,
    description='AI-powered telemetry analysis',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    check_data = BranchPythonOperator(
        task_id='check_parquet_availability',
        python_callable=check_parquet_availability,
    )

    skip_analysis = BashOperator(
        task_id='skip_analysis',
        bash_command='echo "No data available"',
    )

    prepare_db = PythonOperator(
        task_id='prepare_duckdb',
        python_callable=prepare_duckdb,
    )

    run_langgraph = DockerOperator(
        task_id='run_langgraph_agent',
        image='langgraph-agent:latest',
        container_name='langgraph_{{ ts_nodash }}',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='airflow_kafka_opentelemtry_elastic_search_langhchain_telemetry_network',
        mount_tmp_dir=False,
        environment={
            'OLLAMA_BASE_URL': 'http://ollama:11434',
            'DUCKDB_PATH': '/data/telemetry.duckdb',
        },
        mounts=[
            Mount(source=os.path.join(os.environ['PROJECT_DIR'], 'data'),
                  target='/data', type='bind')
        ],
    )

    check_data >> [skip_analysis, prepare_db]
    prepare_db >> run_langgraph
