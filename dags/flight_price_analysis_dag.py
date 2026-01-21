import sys
sys.path.append('/opt/airflow/scripts')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from tasks.ingest_csv_to_mysql import ingest_csv_to_mysql
from tasks.validate_staging_data import validate_staging_data
from tasks.transform_and_compute_kpis import transform_and_compute_kpis

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='flight_price_analysis_bangladesh',
    default_args=default_args,
    description='End-to-end pipeline: Ingest -> Validate -> Transform -> Load & Compute KPIs',
    schedule=None,              # Using Manual trigger for now for now, but '@daily' is an option
    start_date=datetime(2025, 3, 1),
    catchup=False,
    tags=['flight_prices', 'bangladesh', 'analytics'],
    max_active_runs=1,
) as dag:

    start = PythonOperator(
        task_id='start_pipeline',
        python_callable=lambda: print("Starting Flight Price Analysis Pipeline"),
    )

    ingest = PythonOperator(
        task_id='ingest_csv_to_mysql',
        python_callable=ingest_csv_to_mysql,
        provide_context=True,
    )

    validate = PythonOperator(
        task_id='validate_staging_data',
        python_callable=validate_staging_data,
        provide_context=True,
    )

    transform_and_kpis = PythonOperator(
        task_id='transform_and_compute_kpis',
        python_callable=transform_and_compute_kpis,
        provide_context=True,
    )

    finish = PythonOperator(
        task_id='finish_pipeline',
        python_callable=lambda: print("Pipeline completed successfully!"),
    )

    # Dependencies - the real flow
    start >> ingest >> validate >> transform_and_kpis >> finish