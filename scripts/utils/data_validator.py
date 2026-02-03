"""
Data Validator Utility

This module contains all my validation rules for flight price data.
I keep these separate so they're easy to test and modify without touching the main pipeline.
"""

import pandas as pd
import logging
from typing import Tuple, List, Dict, Callable

logger = logging.getLogger(__name__)


# I'm defining my validation rules as a list of dictionaries
# This makes it easy to add, remove, or modify rules without changing the core logic
VALIDATION_RULES: List[Dict] = [
    {
        'name': 'missing_required_values',
        'message': 'Missing required column values',
        'required_cols': ['airline', 'source', 'destination', 'base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt']
    },
    {
        'name': 'negative_or_zero_fare',
        'message': 'Negative or zero fare',
        'fare_cols': ['base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt']
    },
    {
        'name': 'invalid_duration',
        'message': 'Invalid duration',
        'min_hours': 0,
        'max_hours': 40
    },
    {
        'name': 'days_before_departure_range',
        'message': 'Days before departure out of range',
        'min_days': 0,
        'max_days': 365
    },
    {
        'name': 'departure_after_arrival',
        'message': 'Departure after arrival'
    }
]


def check_missing_required(df: pd.DataFrame, required_cols: List[str]) -> pd.Series:
    """
    Checks if any required columns have null values.
    Returns a boolean mask where True means the row has missing values.
    """
    return df[required_cols].isnull().any(axis=1)


def check_fare_validity(df: pd.DataFrame) -> pd.Series:
    """
    Checks if fare values are valid (positive or zero for tax).
    Nobody should be paying zero or negative money for a flight.
    """
    return (df['base_fare_bdt'] <= 0) | (df['tax_surcharge_bdt'] < 0) | (df['total_fare_bdt'] <= 0)


def check_duration_validity(df: pd.DataFrame, min_hours: float = 0, max_hours: float = 40) -> pd.Series:
    """
    Checks if flight duration is within reasonable bounds.
    A flight shouldn't take 0 hours or more than 40 hours for Bangladesh routes.
    """
    return (df['duration_hrs'] <= min_hours) | (df['duration_hrs'] > max_hours)


def check_days_before_departure(df: pd.DataFrame, min_days: int = 0, max_days: int = 365) -> pd.Series:
    """
    Checks if days before departure is within a reasonable range.
    Bookings made more than a year in advance or negative days don't make sense.
    """
    return (df['days_before_departure'] < min_days) | (df['days_before_departure'] > max_days)


def check_departure_arrival_order(df: pd.DataFrame) -> pd.Series:
    """
    Checks if departure time is before arrival time.
    You can't arrive before you depart, that's basic physics.
    """
    if 'departure_date_time' in df.columns and 'arrival_date_time' in df.columns:
        return df['departure_date_time'] >= df['arrival_date_time']
    return pd.Series([False] * len(df), index=df.index)


def validate_dataframe(df: pd.DataFrame, rules: List[Dict] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Runs all validation rules on the dataframe BEFORE inserting anything.
    Returns two dataframes: valid records and invalid records with their reasons.
    
    Args:
        df: The dataframe to validate
        rules: Optional list of validation rules (uses VALIDATION_RULES by default)
        
    Returns:
        Tuple of (valid_df, invalid_df)
    """
    if rules is None:
        rules = VALIDATION_RULES
    
    # Making a copy so I don't mess with the original data
    df = df.copy()
    df['is_valid'] = True
    df['validation_message'] = ''
    
    # Running each validation rule
    for rule in rules:
        rule_name = rule['name']
        message = rule['message']
        
        if rule_name == 'missing_required_values':
            mask = check_missing_required(df, rule['required_cols'])
        elif rule_name == 'negative_or_zero_fare':
            mask = check_fare_validity(df)
        elif rule_name == 'invalid_duration':
            mask = check_duration_validity(df, rule.get('min_hours', 0), rule.get('max_hours', 40))
        elif rule_name == 'days_before_departure_range':
            mask = check_days_before_departure(df, rule.get('min_days', 0), rule.get('max_days', 365))
        elif rule_name == 'departure_after_arrival':
            mask = check_departure_arrival_order(df)
        else:
            logger.warning(f"Unknown validation rule: {rule_name}")
            continue
        
        # Applying the validation result
        df.loc[mask, 'is_valid'] = False
        df.loc[mask, 'validation_message'] += f'; {message}'
    
    # Cleaning up the validation messages by removing the leading semicolon
    df['validation_message'] = df['validation_message'].str.lstrip('; ')
    
    # Splitting into valid and invalid dataframes
    valid_df = df[df['is_valid'] == True].copy()
    invalid_df = df[df['is_valid'] == False].copy()
    
    logger.info(f"Validation results: {len(valid_df)} valid, {len(invalid_df)} invalid")
    
    return valid_df, invalid_df
