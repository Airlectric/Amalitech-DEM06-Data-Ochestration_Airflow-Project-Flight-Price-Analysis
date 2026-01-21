
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.session import create_session
from airflow.models import Connection, Variable
from dotenv import load_dotenv
import os
import json

load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

def create_or_update_connection(**kwargs):
    """Idempotent: create or update Airflow connection using env vars"""
    conn_id = kwargs['conn_id']
    conn_type = kwargs['conn_type']
    host = kwargs['host']
    schema = kwargs['schema']
    login = kwargs.get('login')
    password = kwargs.get('password')
    port = kwargs.get('port')
    extra = kwargs.get('extra')

    with create_session() as session:
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        
        if conn:
            # Update
            conn.conn_type = conn_type
            conn.host = host
            conn.schema = schema
            conn.login = login
            conn.password = password
            conn.port = port
            conn.extra = json.dumps(extra) if extra else None
            action = 'updated'
        else:
            # Create
            conn = Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=host,
                schema=schema,
                login=login,
                password=password,
                port=port,
                extra=json.dumps(extra) if extra else None
            )
            session.add(conn)
            action = 'created'
        
        session.commit()
        print(f"Connection '{conn_id}' {action} successfully")


with DAG(
    dag_id='init_airflow_connections',
    default_args=default_args,
    description='Initialize Airflow connections from .env',
    schedule=None,
    catchup=False,
    tags=['setup', 'connections'],
    max_active_runs=1,
) as dag:

    # MySQL Staging
    create_mysql = PythonOperator(
        task_id='create_mysql_staging_connection',
        python_callable=create_or_update_connection,
        op_kwargs={
            'conn_id': 'mysql_staging',
            'conn_type': 'mysql',
            'host': 'mysql',
            'schema': os.getenv('MYSQL_DATABASE', 'staging_db'),
            'login': os.getenv('MYSQL_USER', 'staging_user'),
            'password': os.getenv('MYSQL_PASSWORD', 'staging_pass'),
            'port': 3306
        }
    )

    # PostgreSQL Analytics
    create_pg_analytics = PythonOperator(
        task_id='create_postgres_analytics_connection',
        python_callable=create_or_update_connection,
        op_kwargs={
            'conn_id': 'postgres_analytics',
            'conn_type': 'postgres',
            'host': 'analytics-postgres',
            'schema': os.getenv('ANALYTICS_POSTGRES_DB', 'analytics_db'),
            'login': os.getenv('ANALYTICS_POSTGRES_USER', 'analytics_user'),
            'password': os.getenv('ANALYTICS_POSTGRES_PASSWORD', 'analytics_pass'),
            'port': 5432
        }
    )

    [create_mysql, create_pg_analytics]