import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def ingest_csv_to_mysql(**context):
    """
    Read CSV to basic type casting to load to MySQL staging table
    Uses pandas + SQLAlchemy for good performance with ~57k rows
    """
    csv_path = Path("/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv")
    
    if not csv_path.exists():
        raise AirflowException(f"CSV file not found: {csv_path}")

    dtype_map = {
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

    logger.info(f"Reading CSV: {csv_path}")
    df = pd.read_csv(
        csv_path,
        dtype=dtype_map,
        parse_dates=False,
        encoding='utf-8'
    )

    # Renaming columns to snake_case (safer for SQL)
    df.columns = df.columns.str.strip().str.lower().str.replace(r'[\s&()-]+', '_', regex=True)

    # Convert datetime columns
    for col in ['departure_date_time', 'arrival_date_time']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    logger.info(f"Loaded {len(df):,} rows. Columns: {list(df.columns)}")

    # Addition of metadata columns
    df = df.assign(
        file_name=csv_path.name,
        source_row_number=lambda x: x.index + 1
    )

    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    engine = mysql_hook.get_sqlalchemy_engine()

    try:
        df.to_sql(
            name='flight_prices_raw',
            con=engine,
            schema='staging',
            if_exists='append',
            index=False,
            chunksize=5000,           # important for memory and the db capacity to help avoid overload
            method='multi'            # faster insert
        )
        logger.info(f"Successfully ingested {len(df):,} rows into staging.flight_prices_raw")
    except Exception as e:
        logger.error(f"Failed to load data to MySQL: {str(e)}")
        raise AirflowException("Data load failed") from e