"""
CSV Reader Utility

This module handles reading and preprocessing CSV files for the flight price pipeline.
I keep the dtype mappings and column transformations here so they're easy to update.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# The dtype mapping for the Bangladesh flight price dataset
# I'm being explicit about types so pandas doesn't guess wrong
FLIGHT_PRICE_DTYPES: Dict[str, str] = {
    'Airline': 'string',
    'Source': 'string',
    'Source Name': 'string',
    'Destination': 'string',
    'Destination Name': 'string',
    'Departure Date & Time': 'string',
    'Arrival Date & Time': 'string',
    'Duration (hrs)': 'float64',
    'Stopovers': 'string',
    'Aircraft Type': 'string',
    'Class': 'string',
    'Booking Source': 'string',
    'Base Fare (BDT)': 'float64',
    'Tax & Surcharge (BDT)': 'float64',
    'Total Fare (BDT)': 'float64',
    'Seasonality': 'string',
    'Days Before Departure': 'int64'
}

# Columns that should be converted to datetime
DATETIME_COLUMNS: List[str] = ['departure_date_time', 'arrival_date_time']


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts column names to snake_case because SQL doesn't like spaces and special chars.
    Also strips any extra whitespace from column names.
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r'[\s&()-]+', '_', regex=True)
        .str.strip('_')
    )
    return df


def convert_datetime_columns(df: pd.DataFrame, columns: List[str] = None) -> pd.DataFrame:
    """
    Converts specified columns to datetime format.
    Uses coerce to handle any malformed dates gracefully.
    """
    if columns is None:
        columns = DATETIME_COLUMNS
    
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    return df


def add_metadata_columns(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """
    Adds metadata columns so I know where each row came from.
    This is helpful for debugging and tracking data lineage.
    """
    df = df.assign(
        file_name=file_name,
        source_row_number=lambda x: x.index + 1
    )
    return df


def read_flight_price_csv(
    csv_path: Path,
    dtype_map: Dict[str, str] = None,
    add_metadata: bool = True
) -> pd.DataFrame:
    """
    Reads a flight price CSV file and preprocesses it for the pipeline.
    
    Args:
        csv_path: Path to the CSV file
        dtype_map: Optional custom dtype mapping (uses FLIGHT_PRICE_DTYPES by default)
        add_metadata: Whether to add file_name and source_row_number columns
        
    Returns:
        Preprocessed DataFrame ready for validation
    """
    if dtype_map is None:
        dtype_map = FLIGHT_PRICE_DTYPES
    
    logger.info(f"Reading CSV: {csv_path}")
    
    df = pd.read_csv(
        csv_path,
        dtype=dtype_map,
        parse_dates=False,
        encoding='utf-8'
    )
    
    # Normalizing column names to snake_case
    df = normalize_column_names(df)
    
    # Converting datetime columns
    df = convert_datetime_columns(df)
    
    # Adding metadata if requested
    if add_metadata:
        df = add_metadata_columns(df, csv_path.name)
    
    logger.info(f"Loaded {len(df):,} rows with columns: {list(df.columns)}")
    
    return df
