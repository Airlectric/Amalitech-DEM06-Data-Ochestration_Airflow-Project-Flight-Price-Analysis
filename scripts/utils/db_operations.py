"""
Database Operations Utility

This module handles all the database insert operations for the pipeline.
I keep these separate so the main task file stays clean and focused on orchestration.
"""

import pandas as pd
import logging
from datetime import datetime
from typing import List
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

# Default connection ID for the staging database
DEFAULT_CONN_ID = 'mysql_staging'
DEFAULT_SCHEMA = 'staging_db'


def get_mysql_engine(conn_id: str = DEFAULT_CONN_ID):
    """
    Gets a SQLAlchemy engine from the Airflow MySQL hook.
    """
    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    return mysql_hook.get_sqlalchemy_engine()


def insert_to_staging(
    df: pd.DataFrame,
    table_name: str = 'flight_prices_raw',
    conn_id: str = DEFAULT_CONN_ID,
    schema: str = DEFAULT_SCHEMA,
    exclude_columns: List[str] = None
) -> int:
    """
    Inserts valid records into the staging table.
    
    Args:
        df: DataFrame with valid records
        table_name: Target table name
        conn_id: Airflow connection ID
        schema: Database schema name
        exclude_columns: Columns to exclude from insert
        
    Returns:
        Number of rows inserted
    """
    if df.empty:
        logger.info("No records to insert into staging")
        return 0
    
    # Removing columns that shouldn't go into staging
    if exclude_columns:
        columns_to_keep = [col for col in df.columns if col not in exclude_columns]
        df = df[columns_to_keep]
    
    engine = get_mysql_engine(conn_id)
    
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False,
            chunksize=5000,
            method='multi'
        )
        logger.info(f"Successfully inserted {len(df):,} records into {schema}.{table_name}")
        return len(df)
    except Exception as e:
        logger.error(f"Failed to insert records into {table_name}: {str(e)}")
        raise AirflowException(f"Insert to {table_name} failed") from e


def insert_to_quarantine(
    df: pd.DataFrame,
    table_name: str = 'flight_prices_quarantine',
    conn_id: str = DEFAULT_CONN_ID,
    schema: str = DEFAULT_SCHEMA
) -> int:
    """
    Inserts invalid records into the quarantine table.
    Adds quarantine-specific metadata columns before inserting.
    
    Args:
        df: DataFrame with invalid records
        table_name: Target quarantine table name
        conn_id: Airflow connection ID
        schema: Database schema name
        
    Returns:
        Number of rows quarantined
    """
    if df.empty:
        logger.info("No records to quarantine")
        return 0
    
    # Adding quarantine-specific columns
    df = df.copy()
    df['quarantine_timestamp'] = datetime.now()
    df['quarantine_reason_summary'] = df['validation_message'].str[:500]
    
    engine = get_mysql_engine(conn_id)
    
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False,
            chunksize=5000,
            method='multi'
        )
        logger.info(f"Successfully quarantined {len(df):,} records into {schema}.{table_name}")
        return len(df)
    except Exception as e:
        logger.error(f"Failed to insert quarantine records: {str(e)}")
        raise AirflowException("Quarantine insert failed") from e
