from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id='check_connections',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['validation', 'connections'],
    max_active_runs=1,
) as dag:

    # Test MySQL Staging connection
    test_mysql = SQLExecuteQueryOperator(
        task_id='test_mysql_staging',
        conn_id='mysql_staging',
        sql='SELECT 1 AS test_connection;',
        hook_params={'autocommit': True},
        return_last=True, 
    )

    # Test PostgreSQL Analytics connection
    test_postgres = SQLExecuteQueryOperator(
        task_id='test_postgres_analytics',
        conn_id='postgres_analytics',
        sql='SELECT 1 AS test_connection;',
        hook_params={'autocommit': True},
        return_last=True,
    )

    [test_mysql, test_postgres]
