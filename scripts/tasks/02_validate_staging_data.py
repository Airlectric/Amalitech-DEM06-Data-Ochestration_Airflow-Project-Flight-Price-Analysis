# scripts/tasks/02_validate_staging_data.py
# ──────────────────────────────────────────────────────────────────────────────
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowSkipException, AirflowException
import logging

logger = logging.getLogger(__name__)

SQL_VALIDATION_CHECKS = [
    """
    UPDATE staging.flight_prices_raw
    SET is_valid = FALSE,
        validation_message = CONCAT(COALESCE(validation_message, ''), '; Missing required column values')
    WHERE airline IS NULL
       OR source IS NULL
       OR destination IS NULL
       OR base_fare_bdt IS NULL
       OR tax_surcharge_bdt IS NULL
       OR total_fare_bdt IS NULL
    """,
    """
    UPDATE staging.flight_prices_raw
    SET is_valid = FALSE,
        validation_message = CONCAT(COALESCE(validation_message, ''), '; Negative or zero fare')
    WHERE base_fare_bdt <= 0
       OR tax_surcharge_bdt < 0
       OR total_fare_bdt <= 0
    """,
    """
    UPDATE staging.flight_prices_raw
    SET is_valid = FALSE,
        validation_message = CONCAT(COALESCE(validation_message, ''), '; Invalid duration')
    WHERE duration_hours <= 0 OR duration_hours > 40
    """,
    """
    UPDATE staging.flight_prices_raw
    SET is_valid = FALSE,
        validation_message = CONCAT(COALESCE(validation_message, ''), '; Days before departure out of range')
    WHERE days_before_departure < 0 OR days_before_departure > 365
    """,
    """
    UPDATE staging.flight_prices_raw
    SET is_valid = FALSE,
        validation_message = CONCAT(COALESCE(validation_message, ''), '; Departure after arrival')
    WHERE departure_datetime >= arrival_datetime
    """
]

def validate_staging_data(**context):
    """Run quality rules and quarantine invalid records then continue pipeline"""
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')

    total_rows = mysql_hook.get_first("SELECT COUNT(*) FROM staging.flight_prices_raw")[0]
    logger.info(f"Starting validation on {total_rows:,} rows...")

    affected_rows_total = 0

    for i, sql in enumerate(SQL_VALIDATION_CHECKS, 1):
        try:
            mysql_hook.run(sql, autocommit=True)
            affected = mysql_hook.get_first("SELECT ROW_COUNT()")[0]
            affected_rows_total += affected
            logger.info(f"Check #{i}: marked {affected:,} rows as invalid")
        except Exception as e:
            logger.error(f"Validation check #{i} failed: {str(e)}")
            raise  # syntax/db errors should still fail

    # Final stats
    invalid_count = mysql_hook.get_first(
        "SELECT COUNT(*) FROM staging.flight_prices_raw WHERE is_valid = FALSE"
    )[0]

    invalid_pct = round(invalid_count / total_rows * 100, 2) if total_rows > 0 else 0

    logger.info(f"Validation finished. Invalid: {invalid_count:,} ({invalid_pct}%)")

    # Quarantine logic – move invalid records to separate table
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count:,} invalid records → quarantining...")


        # Move invalid records
        mysql_hook.run("""
        INSERT INTO staging.flight_prices_quarantine
        SELECT * FROM staging.flight_prices_raw
        WHERE is_valid = FALSE;
        """)

        # Adding quarantine timestamp and reason summary
        mysql_hook.run("""
        UPDATE staging.flight_prices_quarantine
        SET quarantine_timestamp = CURRENT_TIMESTAMP,
            quarantine_reason_summary = LEFT(validation_message, 500);
        """)

        # Removing invalid data from main staging table
        deleted = mysql_hook.run("""
        DELETE FROM staging.flight_prices_raw
        WHERE is_valid = FALSE;
        """, handler=lambda cursor: cursor.rowcount)

        logger.info(f"Moved {invalid_count:,} invalid records to quarantine table")
        logger.info(f"Clean staging table now contains {total_rows - invalid_count:,} valid records")

    # Push metrics for downstream tasks / monitoring
    context['ti'].xcom_push(key='validation_summary', value={
        'total_rows': total_rows,
        'valid_rows': total_rows - invalid_count,
        'invalid_count': invalid_count,
        'invalid_pct': invalid_pct,
        'quarantined': invalid_count > 0
    })

    # Only raising exception if almost everything is bad
    CATASTROPHIC_THRESHOLD = 0.90  # 90% invalid data indicates something is seriously wrong
    if invalid_pct > CATASTROPHIC_THRESHOLD * 100:
        raise AirflowException(
            f"CRITICAL: {invalid_pct}% of records invalid! "
            f"Pipeline stopped to prevent processing completely broken data. "
            f"Check source file and ingestion logic."
        )

    if invalid_count == 0:
        logger.info("All records passed validation ✓")
    else:
        logger.warning(
            f"Pipeline continues with only valid records. "
            f"Check quarantine table for details."
        )