"""
Telemetry Pipeline DAG

Orchestrates data generation, preprocessing, and Parquet writing every 5 minutes.
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    'owner': 'telemetry-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'telemetry_pipeline',
    default_args=default_args,
    description='Car sensor telemetry ingestion pipeline',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: Start generator
    start_generator = DockerOperator(
        task_id='start_generator',
        image='generator:latest',
        container_name='generator_{{ ts_nodash }}',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='airflow_kafka_opentelemtry_elastic_search_langhchain_telemetry_network',
        mount_tmp_dir=False,
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
            'KAFKA_TOPIC': 'car-telemetry-raw',
            'RUN_DURATION': '240',
        },
    )

    # Task 2: Start preprocessor
    start_preprocessor = DockerOperator(
        task_id='start_preprocessor',
        image='preprocessor:latest',
        container_name='preprocessor_{{ ts_nodash }}',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='airflow_kafka_opentelemtry_elastic_search_langhchain_telemetry_network',
        mount_tmp_dir=False,
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
            'KAFKA_INPUT_TOPIC': 'car-telemetry-raw',
            'KAFKA_OUTPUT_TOPIC': 'car-telemetry-enriched',
            'RUN_DURATION': '270',
        },
    )

    # Task 3: Start parquet writer
    start_parquet_writer = DockerOperator(
        task_id='start_parquet_writer',
        image='parquet-writer:latest',
        container_name='parquet_writer_{{ ts_nodash }}',
        api_version='auto',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
        network_mode='airflow_kafka_opentelemtry_elastic_search_langhchain_telemetry_network',
        mount_tmp_dir=False,
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
            'KAFKA_TOPIC': 'car-telemetry-enriched',
            'PARQUET_OUTPUT_PATH': '/data/parquet',
            'RUN_DURATION': '300',
        },
        mounts=[
            Mount(source=os.path.join(os.environ['PROJECT_DIR'], 'data/parquet'),
                  target='/data/parquet', type='bind')
        ],
    )

    [start_generator, start_preprocessor] >> start_parquet_writer
