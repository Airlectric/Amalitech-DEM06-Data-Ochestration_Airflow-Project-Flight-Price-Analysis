
from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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
        sql="""
        CREATE TABLE IF NOT EXISTS analytics.fact_flight_prices (
            flight_price_id       BIGINT PRIMARY KEY,
            airline               VARCHAR(100)      NOT NULL,
            source_iata           VARCHAR(10)       NOT NULL,
            destination_iata      VARCHAR(10)       NOT NULL,
            departure_date        DATE              NOT NULL,
            departure_month       INT               NOT NULL,
            departure_year        INT               NOT NULL,
            class                 VARCHAR(50)       NOT NULL,
            seasonality           VARCHAR(50)       NOT NULL,
            is_peak_season        BOOLEAN           NOT NULL,
            days_before_departure INT               NOT NULL,
            base_fare_bdt         DECIMAL(12,2)     NOT NULL,
            tax_surcharge_bdt     DECIMAL(12,2)     NOT NULL,
            total_fare_bdt        DECIMAL(12,2)     NOT NULL,
            ingestion_timestamp   TIMESTAMP         DEFAULT CURRENT_TIMESTAMP,
            batch_id              VARCHAR(50),
            
            INDEX idx_airline             (airline),
            INDEX idx_route               (source_iata, destination_iata),
            INDEX idx_departure_date      (departure_date),
            INDEX idx_seasonality         (seasonality, is_peak_season)
        );
        """
    )

    create_kpi_avg_airline = SQLExecuteQueryOperator(
        task_id='create_kpi_avg_fare_by_airline',
        conn_id='postgres_analytics',
        sql="""
        CREATE TABLE IF NOT EXISTS analytics.kpi_avg_fare_by_airline (
            airline               VARCHAR(100)      PRIMARY KEY,
            avg_total_fare_bdt    DECIMAL(12,2),
            record_count          BIGINT,
            last_updated          TIMESTAMP         DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    create_kpi_seasonal = SQLExecuteQueryOperator(
        task_id='create_kpi_seasonal_variation',
        conn_id='postgres_analytics',
        sql="""
        CREATE TABLE IF NOT EXISTS analytics.kpi_seasonal_variation (
            seasonality           VARCHAR(50),
            is_peak_season        BOOLEAN,
            avg_total_fare_bdt    DECIMAL(12,2),
            record_count          BIGINT,
            last_updated          TIMESTAMP         DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (seasonality, is_peak_season)
        );
        """
    )

    create_kpi_bookings = SQLExecuteQueryOperator(
        task_id='create_kpi_booking_count_by_airline',
        conn_id='postgres_analytics',
        sql="""
        CREATE TABLE IF NOT EXISTS analytics.kpi_booking_count_by_airline (
            airline               VARCHAR(100)      PRIMARY KEY,
            booking_count         BIGINT,
            last_updated          TIMESTAMP         DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    create_kpi_routes = SQLExecuteQueryOperator(
        task_id='create_kpi_top_routes',
        conn_id='postgres_analytics',
        sql="""
        CREATE TABLE IF NOT EXISTS analytics.kpi_top_routes (
            source_iata           VARCHAR(10),
            destination_iata      VARCHAR(10),
            route_name            VARCHAR(100),
            booking_count         BIGINT,
            avg_total_fare_bdt    DECIMAL(12,2),
            last_updated          TIMESTAMP         DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_iata, destination_iata)
        );
        """
    )

    # Chained them since order doesn't matter much with IF NOT EXISTS
    [
        create_fact_table,
        create_kpi_avg_airline,
        create_kpi_seasonal,
        create_kpi_bookings,
        create_kpi_routes
    ]