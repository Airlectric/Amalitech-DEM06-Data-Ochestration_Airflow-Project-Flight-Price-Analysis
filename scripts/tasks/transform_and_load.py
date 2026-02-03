"""
Transform and Load Data Task

This task handles transforming the validated staging data and loading it into the 
PostgreSQL analytics fact table. The KPI computations are handled by separate tasks
that run in parallel after this one completes.
"""

import sys
sys.path.append('/opt/airflow/scripts')

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowSkipException
from utils.sql_loader import load_sql_file, load_named_sql_queries
from psycopg2.extras import execute_values
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# These are the seasons I consider as peak travel periods
PEAK_SEASONS = {'Winter Holidays', 'Eid'}


def transform_and_load_to_analytics(**context):
    """
    Transforms validated staging data and loads it into the PostgreSQL analytics fact table.
    This is the first step before KPI computations can run.
    """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')

    # Loading query from SQL file to get only valid records
    staging_queries = load_named_sql_queries('staging', 'queries')
    df_query = staging_queries['select_valid_records']

    logger.info("Reading clean data from MySQL staging...")
    df = mysql_hook.get_pandas_df(sql=df_query)

    if df.empty:
        logger.warning("No valid records found in staging. Skipping transformation.")
        raise AirflowSkipException("No valid records to transform")

    logger.info(f"Processing {len(df):,} valid records")

    # Running my core transformations
    df = df.assign(
        # Making sure total_fare is calculated correctly
        total_fare_bdt__corrected=lambda x: x['base_fare_bdt'] + x['tax_surcharge_bdt'],
        
        # Overwriting with the corrected value
        total_fare_bdt=lambda x: x['total_fare_bdt__corrected'],
        
        # Breaking down the date into useful dimensions
        departure_date=lambda x: x['departure_date_time'].dt.date,
        departure_month=lambda x: x['departure_date_time'].dt.month,
        departure_year=lambda x: x['departure_date_time'].dt.year,
        
        # Flagging peak season records for easier analysis
        is_peak_season=lambda x: x['seasonality'].isin(PEAK_SEASONS),
        
        # Adding batch traceability so I can track which run inserted these records
        batch_id=context['run_id'],
        ingestion_timestamp=datetime.utcnow()
    )

    # Cleaning up the temporary column
    df = df.drop(columns=['total_fare_bdt__corrected', 'departure_date_time'])

    # Batch upserting to PostgreSQL using execute_values for better performance
    logger.info("Upserting enriched records to fact_flight_prices...")
    
    batch_upsert_sql = load_sql_file('analytics', 'upsert_fact_flight_prices')
    
    # Setting up the columns in the order the SQL expects them
    columns = [
        'flight_price_id', 'airline', 'source_iata', 'destination_iata',
        'departure_date', 'departure_month', 'departure_year', 'class',
        'seasonality', 'is_peak_season', 'days_before_departure',
        'base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt',
        'ingestion_timestamp', 'batch_id'
    ]
    
    # Converting to list of tuples for execute_values
    records = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
    
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        batch_size = 5000
        total_upserted = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            execute_values(cursor, batch_upsert_sql, batch, page_size=batch_size)
            conn.commit()
            total_upserted += len(batch)
            logger.info(f"Upserted batch: {total_upserted:,}/{len(records):,} records")
        
        logger.info(f"Successfully loaded {len(df):,} records to fact_flight_prices")
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to upsert records: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

    # Pushing record count to XCom so downstream KPI tasks know there's data to process
    context['ti'].xcom_push(key='records_loaded', value=len(df))
    
    return len(df)
