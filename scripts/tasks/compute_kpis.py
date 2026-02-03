"""
KPI Computation Tasks

These are individual KPI tasks that run in parallel after data is loaded into the analytics table.
Each task is independent so if one fails, the others can still complete successfully.
I'm keeping them separate so I can easily add, remove, or modify KPIs without affecting each other.
"""

import sys
sys.path.append('/opt/airflow/scripts')

from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.sql_loader import load_sql_file
import logging

logger = logging.getLogger(__name__)


def _get_postgres_hook():
    """Helper to get the PostgreSQL hook for analytics database."""
    return PostgresHook(postgres_conn_id='postgres_analytics')


def compute_kpi_avg_fare_by_airline(**context):
    """
    Computes the average fare by airline KPI.
    This helps me understand which airlines are more expensive or affordable.
    """
    logger.info("Computing KPI: Average Fare by Airline...")
    
    pg_hook = _get_postgres_hook()
    upsert_sql = load_sql_file('analytics', 'upsert_kpi_avg_fare_by_airline')
    
    try:
        pg_hook.run(upsert_sql)
        logger.info("KPI completed: Average Fare by Airline")
    except Exception as e:
        logger.error(f"Failed to compute Average Fare by Airline KPI: {str(e)}")
        raise


def compute_kpi_seasonal_variation(**context):
    """
    Computes the seasonal fare variation KPI.
    This shows how prices change across different seasons like Eid, Winter Holidays, etc.
    """
    logger.info("Computing KPI: Seasonal Variation...")
    
    pg_hook = _get_postgres_hook()
    upsert_sql = load_sql_file('analytics', 'upsert_kpi_seasonal_variation')
    
    try:
        pg_hook.run(upsert_sql)
        logger.info("KPI completed: Seasonal Variation")
    except Exception as e:
        logger.error(f"Failed to compute Seasonal Variation KPI: {str(e)}")
        raise


def compute_kpi_booking_count_by_airline(**context):
    """
    Computes the booking count by airline KPI.
    This tells me which airlines have the most bookings in the dataset.
    """
    logger.info("Computing KPI: Booking Count by Airline...")
    
    pg_hook = _get_postgres_hook()
    upsert_sql = load_sql_file('analytics', 'upsert_kpi_booking_count_by_airline')
    
    try:
        pg_hook.run(upsert_sql)
        logger.info("KPI completed: Booking Count by Airline")
    except Exception as e:
        logger.error(f"Failed to compute Booking Count by Airline KPI: {str(e)}")
        raise


def compute_kpi_top_routes(**context):
    """
    Computes the top routes KPI.
    This identifies the most popular flight routes based on booking frequency.
    """
    logger.info("Computing KPI: Top Routes...")
    
    pg_hook = _get_postgres_hook()
    upsert_sql = load_sql_file('analytics', 'upsert_kpi_top_routes')
    
    try:
        pg_hook.run(upsert_sql)
        logger.info("KPI completed: Top Routes")
    except Exception as e:
        logger.error(f"Failed to compute Top Routes KPI: {str(e)}")
        raise
