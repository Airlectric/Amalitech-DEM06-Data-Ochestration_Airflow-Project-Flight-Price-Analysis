import sys
sys.path.append('/opt/airflow/scripts')

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from utils.sql_loader import load_sql_file, load_named_sql_queries
from psycopg2.extras import execute_values
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

PEAK_SEASONS = {'Winter Holidays', 'Eid'}  


def transform_and_compute_kpis(**context):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')

    # 1. Load query from SQL file and get only valid records
    staging_queries = load_named_sql_queries('staging', 'queries')
    df_query = staging_queries['select_valid_records']

    logger.info("Reading clean data from MySQL staging...")
    df = mysql_hook.get_pandas_df(sql=df_query)

    if df.empty:
        logger.warning("No valid records found in staging. Skipping transformation.")
        return

    logger.info(f"Processing {len(df):,} valid records")

    # 2. Core transformations
    df = df.assign(
        # Ensuring total_fare is corrected
        total_fare_bdt__corrected = lambda x: x['base_fare_bdt'] + x['tax_surcharge_bdt'],
        
        # Fix potential data issues
        total_fare_bdt= lambda x: x['total_fare_bdt__corrected'],  # overwriting with corrected
        
        # Date dimensions
        departure_date  = lambda x: x['departure_date_time'].dt.date,
        departure_month = lambda x: x['departure_date_time'].dt.month,
        departure_year  = lambda x: x['departure_date_time'].dt.year,
        
        # Peak season flag
        is_peak_season = lambda x: x['seasonality'].isin(PEAK_SEASONS),
        
        # Batch traceability
        batch_id = context['run_id'],
        ingestion_timestamp = datetime.utcnow()
    )

    # Dropping temporary columns
    df = df.drop(columns=['total_fare_bdt__corrected', 'departure_date_time'])

    # 3. Batch upsert to PostgreSQL using execute_values (much faster!)
    logger.info("Upserting enriched records to fact_flight_prices...")
    
    # Load batch upsert SQL from file
    batch_upsert_sql = load_sql_file('analytics', 'upsert_fact_flight_prices')
    
    # Prepare data as list of tuples in correct column order
    columns = [
        'flight_price_id', 'airline', 'source_iata', 'destination_iata',
        'departure_date', 'departure_month', 'departure_year', 'class',
        'seasonality', 'is_peak_season', 'days_before_departure',
        'base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt',
        'ingestion_timestamp', 'batch_id'
    ]
    
    # Convert to list of tuples
    records = [tuple(row[col] for col in columns) for _, row in df.iterrows()]
    
    # Execute batch upsert
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
        
        logger.info(f"Upserted {len(df):,} enriched records to fact_flight_prices")
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to upsert records: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

    # 4. Compute & upsert KPIs using PostgreSQL upsert pattern from SQL files
    kpi_upsert_files = [
        ('upsert_kpi_avg_fare_by_airline', 'Average Fare by Airline'),
        ('upsert_kpi_seasonal_variation', 'Seasonal Variation'),
        ('upsert_kpi_booking_count_by_airline', 'Booking Count by Airline'),
        ('upsert_kpi_top_routes', 'Top Routes')
    ]

    logger.info("Computing and upserting KPI tables...")
    
    for i, (sql_file, kpi_name) in enumerate(kpi_upsert_files, 1):
        upsert_sql = load_sql_file('analytics', sql_file)
        pg_hook.run(upsert_sql)
        logger.info(f"KPI upsert {i}/{len(kpi_upsert_files)} completed: {kpi_name}")

    logger.info("Transformation & KPI computation finished successfully")