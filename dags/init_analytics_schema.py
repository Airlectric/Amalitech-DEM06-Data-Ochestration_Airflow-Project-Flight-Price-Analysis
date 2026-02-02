import sys
sys.path.append('/opt/airflow/scripts')

from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utils.sql_loader import load_sql_file


with DAG(
    dag_id='init_analytics_schema',
    start_date=datetime(2025, 1, 1),
    schedule=None,               # manual trigger only
    catchup=False,
    tags=['init', 'schema', 'setup'],
    default_args={'owner': 'data_engineer'},
) as dag:

    create_fact_table = SQLExecuteQueryOperator(
        task_id='create_fact_flight_prices',
        conn_id='postgres_analytics',
        sql=load_sql_file('analytics', 'create_fact_flight_prices')
    )

    create_kpi_avg_airline = SQLExecuteQueryOperator(
        task_id='create_kpi_avg_fare_by_airline',
        conn_id='postgres_analytics',
        sql=load_sql_file('analytics', 'create_kpi_avg_fare_by_airline')
    )

    create_kpi_seasonal = SQLExecuteQueryOperator(
        task_id='create_kpi_seasonal_variation',
        conn_id='postgres_analytics',
        sql=load_sql_file('analytics', 'create_kpi_seasonal_variation')
    )

    create_kpi_bookings = SQLExecuteQueryOperator(
        task_id='create_kpi_booking_count_by_airline',
        conn_id='postgres_analytics',
        sql=load_sql_file('analytics', 'create_kpi_booking_count_by_airline')
    )

    create_kpi_routes = SQLExecuteQueryOperator(
        task_id='create_kpi_top_routes',
        conn_id='postgres_analytics',
        sql=load_sql_file('analytics', 'create_kpi_top_routes')
    )

    # Chained them since order doesn't matter much with IF NOT EXISTS
    [
        create_fact_table,
        create_kpi_avg_airline,
        create_kpi_seasonal,
        create_kpi_bookings,
        create_kpi_routes
    ]