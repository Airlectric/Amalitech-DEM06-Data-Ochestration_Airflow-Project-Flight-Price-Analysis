from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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
        sql="""
        CREATE TABLE IF NOT EXISTS staging.flight_prices_raw (
            id                    BIGINT AUTO_INCREMENT PRIMARY KEY,
            airline               VARCHAR(100)        NOT NULL,
            source                VARCHAR(10)         NOT NULL,
            source_name           VARCHAR(150),
            destination           VARCHAR(10)         NOT NULL,
            destination_name      VARCHAR(150),
            departure_datetime    DATETIME            NOT NULL,
            arrival_datetime      DATETIME            NOT NULL,
            duration_hours        DECIMAL(6,2)        NOT NULL,
            stopovers             VARCHAR(20)         NOT NULL,
            aircraft_type         VARCHAR(100),
            class                 VARCHAR(50)         NOT NULL,
            booking_source        VARCHAR(100),
            base_fare_bdt         DECIMAL(12,2)       NOT NULL,
            tax_surcharge_bdt     DECIMAL(12,2)       NOT NULL,
            total_fare_bdt        DECIMAL(12,2)       NOT NULL,
            seasonality           VARCHAR(50)         NOT NULL,
            days_before_departure INT                 NOT NULL,
            
            ingestion_timestamp   TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,
            file_name             VARCHAR(255),
            source_row_number     BIGINT,
            
            is_valid              BOOLEAN             DEFAULT TRUE,
            validation_message    TEXT,
            
            INDEX idx_route       (source, destination),
            INDEX idx_departure   (departure_datetime),
            INDEX idx_seasonality (seasonality)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    )

    create_quarantine_table = SQLExecuteQueryOperator(
        task_id='create_flight_prices_quarantine',
        conn_id='mysql_staging',
        sql="""
        CREATE TABLE IF NOT EXISTS staging.flight_prices_quarantine (
            -- Same structure as raw table + quarantine-specific columns
            id                    BIGINT,
            airline               VARCHAR(100)        NOT NULL,
            source                VARCHAR(10)         NOT NULL,
            source_name           VARCHAR(150),
            destination           VARCHAR(10)         NOT NULL,
            destination_name      VARCHAR(150),
            departure_datetime    DATETIME            NOT NULL,
            arrival_datetime      DATETIME            NOT NULL,
            duration_hours        DECIMAL(6,2)        NOT NULL,
            stopovers             VARCHAR(20)         NOT NULL,
            aircraft_type         VARCHAR(100),
            class                 VARCHAR(50)         NOT NULL,
            booking_source        VARCHAR(100),
            base_fare_bdt         DECIMAL(12,2)       NOT NULL,
            tax_surcharge_bdt     DECIMAL(12,2)       NOT NULL,
            total_fare_bdt        DECIMAL(12,2)       NOT NULL,
            seasonality           VARCHAR(50)         NOT NULL,
            days_before_departure INT                 NOT NULL,
            
            ingestion_timestamp   TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,
            file_name             VARCHAR(255),
            source_row_number     BIGINT,
            
            is_valid              BOOLEAN             DEFAULT FALSE,   -- always false here
            validation_message    TEXT,
            
            -- Quarantine metadata
            quarantine_timestamp  TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,
            quarantine_reason_summary  VARCHAR(500),   -- shortened version of validation_message
            batch_id              VARCHAR(50),        -- optional: run_id or date
            quarantine_notes      TEXT,               -- for manual comments later
            
            PRIMARY KEY (id),
            INDEX idx_quarantine_ts (quarantine_timestamp),
            INDEX idx_reason        (quarantine_reason_summary(100))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    )

    # Execution order
    create_raw_table >> create_quarantine_table