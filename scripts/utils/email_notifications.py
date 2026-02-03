"""
Email Notification Utility

This module handles sending email alerts for the pipeline.
I use this to notify when there are critical issues or to send validation summaries.

Uses direct SMTP connection instead of Airflow's send_email utility for reliability.
Reads SMTP settings from environment variables:
- AIRFLOW__SMTP__SMTP_HOST
- AIRFLOW__SMTP__SMTP_PORT
- AIRFLOW__SMTP__SMTP_USER
- AIRFLOW__SMTP__SMTP_PASSWORD
- AIRFLOW__SMTP__SMTP_MAIL_FROM
"""

import logging
import os
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Default recipients for alerts - you can override these when calling the functions
DEFAULT_ALERT_RECIPIENTS = []  # Add your email addresses here, e.g., ['data-team@company.com']

# Retry settings for email sending
MAX_EMAIL_RETRIES = 3
RETRY_DELAY_SECONDS = 2


def _get_smtp_config() -> Dict:
    """
    Gets SMTP configuration from environment variables.
    Returns a dict with host, port, user, password, and mail_from.
    """
    return {
        'host': os.environ.get('AIRFLOW__SMTP__SMTP_HOST', 'smtp.gmail.com'),
        'port': int(os.environ.get('AIRFLOW__SMTP__SMTP_PORT', 587)),
        'user': os.environ.get('AIRFLOW__SMTP__SMTP_USER', ''),
        'password': os.environ.get('AIRFLOW__SMTP__SMTP_PASSWORD', ''),
        'mail_from': os.environ.get('AIRFLOW__SMTP__SMTP_MAIL_FROM', ''),
        'starttls': os.environ.get('AIRFLOW__SMTP__SMTP_STARTTLS', 'True').lower() == 'true',
    }


def _send_email_smtp(recipients: List[str], subject: str, html_content: str) -> bool:
    """
    Sends an email using direct SMTP connection.
    This bypasses Airflow's send_email utility for better reliability with Gmail.
    """
    config = _get_smtp_config()
    
    if not config['user'] or not config['password']:
        logger.warning("SMTP credentials not configured, skipping email")
        return False
    
    # Build the email message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = config['mail_from'] or config['user']
    msg['To'] = ', '.join(recipients)
    
    # Attach HTML content
    html_part = MIMEText(html_content, 'html')
    msg.attach(html_part)
    
    try:
        # Connect and send
        server = smtplib.SMTP(config['host'], config['port'], timeout=30)
        server.ehlo()
        
        if config['starttls']:
            server.starttls()
            server.ehlo()
        
        server.login(config['user'], config['password'])
        server.sendmail(config['mail_from'] or config['user'], recipients, msg.as_string())
        server.quit()
        
        logger.info(f"Email sent successfully to {recipients}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        raise


def _send_with_retry(recipients: List[str], subject: str, html_content: str) -> bool:
    """
    Attempts to send an email with retries.
    Sometimes SMTP connections can be flaky, so retrying helps.
    """
    last_error = None
    
    for attempt in range(1, MAX_EMAIL_RETRIES + 1):
        try:
            return _send_email_smtp(recipients, subject, html_content)
        except Exception as e:
            error_str = str(e)
            last_error = e
            logger.warning(f"Email attempt {attempt}/{MAX_EMAIL_RETRIES} failed: {error_str}")
            
            if attempt < MAX_EMAIL_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
    
    logger.error(f"Failed to send email after {MAX_EMAIL_RETRIES} attempts. Last error: {str(last_error)}")
    return False


def send_validation_summary_email(
    summary: Dict,
    recipients: List[str] = None,
    dag_id: str = None,
    run_id: str = None
) -> bool:
    """
    Sends an email with the validation summary after processing a dataset.
    Good for keeping the team informed about data quality.
    
    Args:
        summary: Dictionary containing validation metrics (from XCom)
        recipients: List of email addresses to send to
        dag_id: The DAG ID for context
        run_id: The run ID for context
        
    Returns:
        True if email sent successfully, False otherwise
    """
    recipients = recipients or DEFAULT_ALERT_RECIPIENTS
    if not recipients:
        logger.info("No recipients configured for validation summary email")
        return False
    
    # Building the email content
    file_name = summary.get('file_name', 'Unknown')
    total_rows = summary.get('total_rows', 0)
    valid_rows = summary.get('valid_rows', 0)
    invalid_count = summary.get('invalid_count', 0)
    invalid_pct = summary.get('invalid_pct', 0)
    quarantined = summary.get('quarantined', False)
    
    # Deciding the subject based on data quality
    if invalid_pct > 50:
        status_emoji = "⚠️"
        status_text = "HIGH INVALID RATE"
    elif invalid_pct > 10:
        status_emoji = "📊"
        status_text = "Moderate Issues"
    else:
        status_emoji = "✅"
        status_text = "Success"
    
    subject = f"{status_emoji} Flight Data Validation {status_text}: {file_name}"
    
    html_content = f"""
    <html>
    <body>
        <h2>Flight Price Data Validation Summary</h2>
        <p><strong>File:</strong> {file_name}</p>
        <p><strong>Processed at:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        
        <h3>Results</h3>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse;">
            <tr style="background-color: #f2f2f2;">
                <th>Metric</th>
                <th>Value</th>
            </tr>
            <tr>
                <td>Total Records</td>
                <td><strong>{total_rows:,}</strong></td>
            </tr>
            <tr style="background-color: #d4edda;">
                <td>Valid Records</td>
                <td><strong>{valid_rows:,}</strong></td>
            </tr>
            <tr style="background-color: {'#f8d7da' if invalid_count > 0 else '#d4edda'};">
                <td>Invalid Records (Quarantined)</td>
                <td><strong>{invalid_count:,}</strong> ({invalid_pct}%)</td>
            </tr>
        </table>
        
        <p>{'⚠️ Invalid records have been moved to the quarantine table for review.' if quarantined else '✅ All records passed validation.'}</p>
        
        <hr>
        <p style="color: #666; font-size: 12px;">
            DAG: {dag_id or 'N/A'}<br>
            Run ID: {run_id or 'N/A'}
        </p>
    </body>
    </html>
    """
    
    try:
        return _send_with_retry(recipients, subject, html_content)
    except Exception as e:
        logger.error(f"Failed to send validation summary email: {str(e)}")
        return False


def send_critical_alert_email(
    error_message: str,
    summary: Dict = None,
    recipients: List[str] = None,
    dag_id: str = None,
    run_id: str = None
) -> bool:
    """
    Sends a critical alert email when something goes seriously wrong.
    Like when 90%+ of the data fails validation - that needs immediate attention.
    
    Args:
        error_message: The error message to include
        summary: Optional validation summary dict
        recipients: List of email addresses to send to
        dag_id: The DAG ID for context
        run_id: The run ID for context
        
    Returns:
        True if email sent successfully, False otherwise
    """
    recipients = recipients or DEFAULT_ALERT_RECIPIENTS
    if not recipients:
        logger.info("No recipients configured for critical alert email")
        return False
    
    subject = f"🚨 CRITICAL: Flight Data Pipeline Failure"
    
    # Building summary section if provided
    summary_html = ""
    if summary:
        summary_html = f"""
        <h3>Validation Summary</h3>
        <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse;">
            <tr><td>File</td><td>{summary.get('file_name', 'N/A')}</td></tr>
            <tr><td>Total Records</td><td>{summary.get('total_rows', 0):,}</td></tr>
            <tr><td>Valid Records</td><td>{summary.get('valid_rows', 0):,}</td></tr>
            <tr><td>Invalid Records</td><td>{summary.get('invalid_count', 0):,} ({summary.get('invalid_pct', 0)}%)</td></tr>
        </table>
        """
    
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <div style="background-color: #f8d7da; border: 2px solid #f5c6cb; padding: 20px; border-radius: 5px;">
            <h2 style="color: #721c24;">🚨 Critical Pipeline Alert</h2>
            <p style="color: #721c24; font-size: 16px;"><strong>{error_message}</strong></p>
        </div>
        
        {summary_html}
        
        <h3>Required Actions</h3>
        <ul>
            <li>Check the source data file for issues</li>
            <li>Review the quarantine table for details on invalid records</li>
            <li>Verify the data ingestion process</li>
            <li>Check if there were any upstream data quality issues</li>
        </ul>
        
        <hr>
        <p style="color: #666; font-size: 12px;">
            Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}<br>
            DAG: {dag_id or 'N/A'}<br>
            Run ID: {run_id or 'N/A'}
        </p>
    </body>
    </html>
    """
    
    try:
        return _send_with_retry(recipients, subject, html_content)
    except Exception as e:
        logger.error(f"Failed to send critical alert email: {str(e)}")
        return False


def send_pipeline_success_email(
    summary: Dict,
    recipients: List[str] = None,
    dag_id: str = None,
    run_id: str = None
) -> bool:
    """
    Sends a success notification email when the pipeline completes.
    This is optional - you might only want alerts for failures.
    
    Args:
        summary: Validation summary dict
        recipients: List of email addresses
        dag_id: The DAG ID
        run_id: The run ID
        
    Returns:
        True if sent successfully
    """
    # This just calls the validation summary email with success context
    return send_validation_summary_email(
        summary=summary,
        recipients=recipients,
        dag_id=dag_id,
        run_id=run_id
    )
