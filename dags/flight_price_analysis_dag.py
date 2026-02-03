import sys
sys.path.append('/opt/airflow/scripts')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Importing the task functions
from tasks.validate_staging_data import validate_staging_data
from tasks.transform_and_load import transform_and_load_to_analytics
from tasks.compute_kpis import (
    compute_kpi_avg_fare_by_airline,
    compute_kpi_seasonal_variation,
    compute_kpi_booking_count_by_airline,
    compute_kpi_top_routes
)

default_args = {
    'owner': 'Daniel Doe',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='flight_price_analysis_bangladesh',
    default_args=default_args,
    description='End-to-end pipeline: Validate & Ingest -> Transform -> Load -> Compute KPIs (parallel)',
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

    # This task does validation FIRST, then inserts valid data to staging and invalid to quarantine
    # It also automatically finds the most recent unprocessed dataset
    validate_and_ingest = PythonOperator(
        task_id='validate_and_ingest_data',
        python_callable=validate_staging_data,
    )

    # Transform staging data and load into the PostgreSQL analytics fact table
    transform_and_load = PythonOperator(
        task_id='transform_and_load_to_analytics',
        python_callable=transform_and_load_to_analytics,
    )

    # KPI tasks run in parallel after data is loaded
    # If one fails, the others can still complete successfully
    kpi_avg_fare = PythonOperator(
        task_id='kpi_avg_fare_by_airline',
        python_callable=compute_kpi_avg_fare_by_airline,
    )

    kpi_seasonal = PythonOperator(
        task_id='kpi_seasonal_variation',
        python_callable=compute_kpi_seasonal_variation,
    )

    kpi_booking_count = PythonOperator(
        task_id='kpi_booking_count_by_airline',
        python_callable=compute_kpi_booking_count_by_airline,
    )

    kpi_top_routes = PythonOperator(
        task_id='kpi_top_routes',
        python_callable=compute_kpi_top_routes,
    )

    finish = PythonOperator(
        task_id='finish_pipeline',
        python_callable=lambda: print("Pipeline completed successfully!"),
        # This task runs even if some KPI tasks fail, as long as at least one succeeds
        trigger_rule='none_failed_min_one_success',
    )

    # The flow: validate -> transform & load -> KPIs in parallel -> finish
    start >> validate_and_ingest >> transform_and_load
    
    # All KPI tasks run in parallel after data is loaded
    transform_and_load >> [kpi_avg_fare, kpi_seasonal, kpi_booking_count, kpi_top_routes]
    
    # Finish waits for all KPI tasks (but uses none_failed_min_one_success trigger rule)
    [kpi_avg_fare, kpi_seasonal, kpi_booking_count, kpi_top_routes] >> finish