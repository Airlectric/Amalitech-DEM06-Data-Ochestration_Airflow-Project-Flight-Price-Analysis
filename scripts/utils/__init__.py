"""
Utility module for the Airflow data pipeline.
Contains reusable components for data processing, validation, and database operations.
"""

from .sql_loader import SQLLoader, sql_loader, load_sql_file, load_sql_statements, load_named_sql_queries
from .dataset_tracker import get_unprocessed_datasets, mark_as_processed, get_processed_files
from .csv_reader import read_flight_price_csv, normalize_column_names, FLIGHT_PRICE_DTYPES
from .data_validator import validate_dataframe, VALIDATION_RULES
from .db_operations import insert_to_staging, insert_to_quarantine, get_mysql_engine

__all__ = [
    # SQL Loader
    'SQLLoader',
    'sql_loader',
    'load_sql_file',
    'load_sql_statements',
    'load_named_sql_queries',
    # Dataset Tracker
    'get_unprocessed_datasets',
    'mark_as_processed',
    'get_processed_files',
    # CSV Reader
    'read_flight_price_csv',
    'normalize_column_names',
    'FLIGHT_PRICE_DTYPES',
    # Data Validator
    'validate_dataframe',
    'VALIDATION_RULES',
    # Database Operations
    'insert_to_staging',
    'insert_to_quarantine',
    'get_mysql_engine',
]
