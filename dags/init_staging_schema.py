import sys
sys.path.append('/opt/airflow/scripts')

from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utils.sql_loader import load_sql_file


with DAG(
    dag_id='init_staging_schema',
    start_date=datetime(2025, 1, 1),
    schedule=None,                 # Manual trigger only
    catchup=False,
    tags=['init', 'schema', 'setup'],
    default_args={'owner': 'data_engineer'},
) as dag:

    create_raw_table = SQLExecuteQueryOperator(
        task_id='create_flight_prices_raw',
        conn_id='mysql_staging',
        sql=load_sql_file('staging', 'create_flight_prices_raw')
    )

    create_quarantine_table = SQLExecuteQueryOperator(
        task_id='create_flight_prices_quarantine',
        conn_id='mysql_staging',
        sql=load_sql_file('staging', 'create_flight_prices_quarantine')
    )

    # Execution order
    create_raw_table >> create_quarantine_table