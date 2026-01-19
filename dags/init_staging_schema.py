from datetime import datetime
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator

with DAG(
    dag_id='init_staging_schema',
    start_date=datetime(2025, 1, 1),
    schedule=None,                 # only manual trigger
    catchup=False,
    tags=['init', 'schema'],
) as dag:

    create_staging_table = MySqlOperator(
        task_id='create_flight_prices_raw_table',
        mysql_conn_id='mysql_staging',
        sql="""
        CREATE TABLE IF NOT EXISTS staging.flight_prices_raw (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            
            airline VARCHAR(100) NOT NULL,
            source VARCHAR(10) NOT NULL,
            source_name VARCHAR(150),
            destination VARCHAR(10) NOT NULL,
            destination_name VARCHAR(150),
            
            departure_datetime DATETIME NOT NULL,
            arrival_datetime DATETIME NOT NULL,
            duration_hours DECIMAL(6,2) NOT NULL,
            
            stopovers VARCHAR(20) NOT NULL,
            aircraft_type VARCHAR(100),
            class VARCHAR(50) NOT NULL,
            
            booking_source VARCHAR(100),
            base_fare_bdt DECIMAL(12,2) NOT NULL,
            tax_surcharge_bdt DECIMAL(12,2) NOT NULL,
            total_fare_bdt DECIMAL(12,2) NOT NULL,
            
            seasonality VARCHAR(50) NOT NULL,
            days_before_departure INT NOT NULL,
            
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_name VARCHAR(255),
            source_row_number BIGINT,
            
            is_valid BOOLEAN DEFAULT TRUE,
            validation_message TEXT,
            
            INDEX idx_airline (airline),
            INDEX idx_route (source, destination),
            INDEX idx_seasonality (seasonality),
            INDEX idx_departure_dt (departure_datetime)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    )


    create_staging_table