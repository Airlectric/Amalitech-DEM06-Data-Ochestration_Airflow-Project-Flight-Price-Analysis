"""
Database Operations Utility

This module handles all the database insert operations for the pipeline.
I keep these separate so the main task file stays clean and focused on orchestration.
Uses upsert logic to prevent duplicate records based on file_name + source_row_number.
"""

import pandas as pd
import logging
from datetime import datetime
from typing import List, Tuple
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Default connection ID for the staging database
DEFAULT_CONN_ID = 'mysql_staging'
DEFAULT_SCHEMA = 'staging_db'

# These columns uniquely identify a record from the source file
UNIQUE_KEY_COLUMNS = ['file_name', 'source_row_number']


def get_mysql_hook(conn_id: str = DEFAULT_CONN_ID) -> MySqlHook:
    """Gets the MySQL hook for database operations."""
    return MySqlHook(mysql_conn_id=conn_id)


def get_mysql_engine(conn_id: str = DEFAULT_CONN_ID):
    """
    Gets a SQLAlchemy engine from the Airflow MySQL hook.
    """
    mysql_hook = MySqlHook(mysql_conn_id=conn_id)
    return mysql_hook.get_sqlalchemy_engine()


def _build_upsert_query(
    table_name: str,
    schema: str,
    columns: List[str],
    update_columns: List[str] = None
) -> str:
    """
    Builds a MySQL INSERT ... ON DUPLICATE KEY UPDATE query.
    This way if a record already exists (same file_name + source_row_number), it gets updated instead of failing.
    """
    full_table = f"{schema}.{table_name}"
    cols_str = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    
    # If no update columns specified, update all except the unique key columns
    if update_columns is None:
        update_columns = [col for col in columns if col not in UNIQUE_KEY_COLUMNS]
    
    # Building the ON DUPLICATE KEY UPDATE part
    update_parts = [f"{col} = VALUES({col})" for col in update_columns]
    update_str = ', '.join(update_parts)
    
    query = f"""
        INSERT INTO {full_table} ({cols_str})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_str}
    """
    return query


def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    conn_id: str = DEFAULT_CONN_ID,
    schema: str = DEFAULT_SCHEMA,
    exclude_columns: List[str] = None,
    batch_size: int = 5000
) -> Tuple[int, int]:
    """
    Upserts records to a table using INSERT ON DUPLICATE KEY UPDATE.
    Duplicates are identified by file_name + source_row_number.
    
    Args:
        df: DataFrame to upsert
        table_name: Target table name
        conn_id: Airflow connection ID
        schema: Database schema name
        exclude_columns: Columns to exclude from insert
        batch_size: Number of records per batch
        
    Returns:
        Tuple of (total_processed, duplicates_updated)
    """
    if df.empty:
        logger.info(f"No records to upsert into {table_name}")
        return 0, 0
    
    df = df.copy()
    
    # Removing columns that shouldn't go into the table
    if exclude_columns:
        columns_to_keep = [col for col in df.columns if col not in exclude_columns]
        df = df[columns_to_keep]
    
    columns = list(df.columns)
    query = _build_upsert_query(table_name, schema, columns)
    
    mysql_hook = get_mysql_hook(conn_id)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    
    total_processed = 0
    
    try:
        # Converting DataFrame to list of tuples for batch insert
        records = [tuple(row) for row in df.values]
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            cursor.executemany(query, batch)
            conn.commit()
            total_processed += len(batch)
            logger.info(f"Upserted batch: {total_processed:,}/{len(records):,} records")
        
        # Getting info about how many were actual inserts vs updates
        # MySQL doesn't give us this directly, but we can estimate
        logger.info(f"Successfully upserted {len(df):,} records into {schema}.{table_name}")
        
        return total_processed, 0  # We don't have exact duplicate count, but operation succeeded
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to upsert records into {table_name}: {str(e)}")
        raise AirflowException(f"Upsert to {table_name} failed") from e
    finally:
        cursor.close()
        conn.close()


def insert_to_staging(
    df: pd.DataFrame,
    table_name: str = 'flight_prices_raw',
    conn_id: str = DEFAULT_CONN_ID,
    schema: str = DEFAULT_SCHEMA,
    exclude_columns: List[str] = None
) -> int:
    """
    Upserts valid records into the staging table.
    Uses INSERT ON DUPLICATE KEY UPDATE to handle duplicates gracefully.
    Duplicates are identified by file_name + source_row_number.
    
    Args:
        df: DataFrame with valid records
        table_name: Target table name
        conn_id: Airflow connection ID
        schema: Database schema name
        exclude_columns: Columns to exclude from insert
        
    Returns:
        Number of rows processed
    """
    if df.empty:
        logger.info("No records to insert into staging")
        return 0
    
    total, _ = upsert_dataframe(
        df=df,
        table_name=table_name,
        conn_id=conn_id,
        schema=schema,
        exclude_columns=exclude_columns
    )
    
    return total


def insert_to_quarantine(
    df: pd.DataFrame,
    table_name: str = 'flight_prices_quarantine',
    conn_id: str = DEFAULT_CONN_ID,
    schema: str = DEFAULT_SCHEMA
) -> int:
    """
    Upserts invalid records into the quarantine table.
    Uses INSERT ON DUPLICATE KEY UPDATE to handle duplicates gracefully.
    If the same record (file_name + source_row_number) fails validation again, 
    it updates the existing quarantine entry with the latest info.
    
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
    
    # Removing the 'id' column if it exists since quarantine table has its own auto-increment id
    exclude_cols = ['id'] if 'id' in df.columns else None
    
    total, _ = upsert_dataframe(
        df=df,
        table_name=table_name,
        conn_id=conn_id,
        schema=schema,
        exclude_columns=exclude_cols
    )
    
    logger.info(f"Successfully quarantined {total:,} records into {schema}.{table_name}")
    return total
