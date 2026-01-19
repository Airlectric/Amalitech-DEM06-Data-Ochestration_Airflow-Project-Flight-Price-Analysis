from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

PEAK_SEASONS = {'Winter Holidays', 'Eid'}  

def transform_and_compute_kpis(**context):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytics')

    # 1. Get only valid records
    df_query = """
    SELECT 
        id AS flight_price_id,
        airline,
        source,
        destination,
        departure_datetime,
        class,
        seasonality,
        days_before_departure,
        base_fare_bdt,
        tax_surcharge_bdt,
        total_fare_bdt
    FROM staging.flight_prices_raw
    WHERE is_valid = TRUE
    """

    logger.info("Reading clean data from MySQL staging...")
    df = mysql_hook.get_pandas_df(sql=df_query)

    if df.empty:
        logger.warning("No valid records found in staging. Skipping transformation.")
        return

    logger.info(f"Processing {len(df):,} valid records")

    # 2. Core transformations
    df = df.assign(
        # Ensuring total_fare is corrected
        total_fare_bdt_corrected = lambda x: x['base_fare_bdt'] + x['tax_surcharge_bdt'],
        
        # Fix potential data issues
        total_fare_bdt = lambda x: x['total_fare_bdt_corrected'],  # overwriting with corrected
        
        # Date dimensions
        departure_date  = lambda x: x['departure_datetime'].dt.date,
        departure_month = lambda x: x['departure_datetime'].dt.month,
        departure_year  = lambda x: x['departure_datetime'].dt.year,
        
        # Peak season flag
        is_peak_season = lambda x: x['seasonality'].isin(PEAK_SEASONS),
        
        # Batch traceability
        batch_id = context['run_id'],
        ingestion_timestamp = datetime.utcnow()
    )

    # Dropping temporary columns
    df = df.drop(columns=['total_fare_bdt_corrected', 'departure_datetime'])

    # 3. Loading enriched data to PostgreSQL
    pg_engine = pg_hook.get_sqlalchemy_engine()
    
    df.to_sql(
        name='fact_flight_prices',
        con=pg_engine,
        schema='analytics',
        if_exists='append',
        index=False,
        chunksize=10000,
        method='multi'
    )

    logger.info(f"Loaded {len(df):,} enriched records to analytics.fact_flight_prices")

    # 4. Compute & upsert KPIs using PostgreSQL upsert pattern
    upsert_queries = [
        # Average Fare by Airline
        """
        INSERT INTO analytics.kpi_avg_fare_by_airline
            (airline, avg_total_fare_bdt, record_count, last_updated)
        SELECT 
            airline,
            ROUND(AVG(total_fare_bdt), 2),
            COUNT(*),
            CURRENT_TIMESTAMP
        FROM analytics.fact_flight_prices
        GROUP BY airline
        ON CONFLICT (airline) 
            DO UPDATE SET
                avg_total_fare_bdt = EXCLUDED.avg_total_fare_bdt,
                record_count = EXCLUDED.record_count,
                last_updated = EXCLUDED.last_updated;
        """,

        # Seasonal Variation
        """
        INSERT INTO analytics.kpi_seasonal_variation
            (seasonality, is_peak_season, avg_total_fare_bdt, record_count, last_updated)
        SELECT 
            seasonality,
            is_peak_season,
            ROUND(AVG(total_fare_bdt), 2),
            COUNT(*),
            CURRENT_TIMESTAMP
        FROM analytics.fact_flight_prices
        GROUP BY seasonality, is_peak_season
        ON CONFLICT (seasonality, is_peak_season) 
            DO UPDATE SET
                avg_total_fare_bdt = EXCLUDED.avg_total_fare_bdt,
                record_count = EXCLUDED.record_count,
                last_updated = EXCLUDED.last_updated;
        """,

        # Booking Count by Airline (simple count of records)
        """
        INSERT INTO analytics.kpi_booking_count_by_airline
            (airline, booking_count, last_updated)
        SELECT 
            airline,
            COUNT(*),
            CURRENT_TIMESTAMP
        FROM analytics.fact_flight_prices
        GROUP BY airline
        ON CONFLICT (airline) 
            DO UPDATE SET
                booking_count = EXCLUDED.booking_count,
                last_updated = EXCLUDED.last_updated;
        """,

        # Top Routes
        """
        INSERT INTO analytics.kpi_top_routes
            (source_iata, destination_iata, route_name, booking_count, avg_total_fare_bdt, last_updated)
        SELECT 
            source_iata,
            destination_iata,
            CONCAT(source_iata, ' to ', destination_iata) AS route_name,
            COUNT(*) AS booking_count,
            ROUND(AVG(total_fare_bdt), 2) AS avg_total_fare_bdt,
            CURRENT_TIMESTAMP
        FROM analytics.fact_flight_prices
        GROUP BY source_iata, destination_iata
        ORDER BY booking_count DESC
        LIMIT 20
        ON CONFLICT (source_iata, destination_iata) 
            DO UPDATE SET
                booking_count = EXCLUDED.booking_count,
                avg_total_fare_bdt = EXCLUDED.avg_total_fare_bdt,
                route_name = EXCLUDED.route_name,
                last_updated = EXCLUDED.last_updated;
        """
    ]

    logger.info("Computing and upserting KPI tables...")
    for i, query in enumerate(upsert_queries, 1):
        pg_hook.run(query)
        logger.info(f"KPI upsert query {i}/{len(upsert_queries)} completed")

    logger.info("Transformation & KPI computation finished successfully")