import sys
sys.path.append('/opt/airflow/scripts')

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowSkipException, AirflowException
from utils.sql_loader import load_sql_statements, load_named_sql_queries
import logging

logger = logging.getLogger(__name__)


def validate_staging_data(**context):
    """Run quality rules and quarantine invalid records then continue pipeline"""
    mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
    
    # Load queries from SQL files
    staging_queries = load_named_sql_queries('staging', 'queries')
    
    # Get total row count
    total_rows = mysql_hook.get_first(staging_queries['count_total_rows'])[0]
    logger.info(f"Starting validation on {total_rows:,} rows...")

    # Load and run validation checks from SQL file
    validation_checks = load_sql_statements('staging', 'validation_checks')
    
    affected_rows_total = 0
    for i, sql in enumerate(validation_checks, 1):
        try:
            mysql_hook.run(sql, autocommit=True)
            affected = mysql_hook.get_first(staging_queries['get_row_count'])[0]
            affected_rows_total += affected
            logger.info(f"Check #{i}: marked {affected:,} rows as invalid")
        except Exception as e:
            logger.error(f"Validation check #{i} failed: {str(e)}")
            raise  # syntax/db errors should still fail

    # Final stats
    invalid_count = mysql_hook.get_first(staging_queries['count_invalid_rows'])[0]
    invalid_pct = round(invalid_count / total_rows * 100, 2) if total_rows > 0 else 0

    logger.info(f"Validation finished. Invalid: {invalid_count:,} ({invalid_pct}%)")

    # Quarantine logic – upsert invalid records to separate table
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count:,} invalid records → quarantining...")

        # Load quarantine operations (upsert + delete)
        quarantine_ops = load_sql_statements('staging', 'quarantine_operations')
        
        # Execute upsert to quarantine (first statement)
        mysql_hook.run(quarantine_ops[0], autocommit=True)
        logger.info(f"Upserted {invalid_count:,} records to quarantine table")
        
        # Delete from raw table (second statement)
        mysql_hook.run(quarantine_ops[1], autocommit=True)
        logger.info(f"Removed invalid records from raw staging table")
        
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