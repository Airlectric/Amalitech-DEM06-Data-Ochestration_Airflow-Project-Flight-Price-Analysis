"""
SQL Loader Utility Module

This module provides functions to load and execute SQL files from the sql/ directory.
It supports multi-statement SQL files with separators and named queries.
"""

from pathlib import Path
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

# Base path for SQL files (relative to Airflow container)
SQL_BASE_PATH = Path('/opt/airflow/sql')


def get_sql_file_path(category: str, filename: str) -> Path:
    """
    Get the full path to a SQL file.
    
    Args:
        category: The SQL category folder (e.g., 'staging', 'analytics')
        filename: The SQL filename (with or without .sql extension)
    
    Returns:
        Path object to the SQL file
    """
    if not filename.endswith('.sql'):
        filename = f"{filename}.sql"
    return SQL_BASE_PATH / category / filename


def load_sql_file(category: str, filename: str) -> str:
    """
    Load a single SQL file and return its contents.
    
    Args:
        category: The SQL category folder (e.g., 'staging', 'analytics')
        filename: The SQL filename
    
    Returns:
        SQL content as a string
    
    Raises:
        FileNotFoundError: If the SQL file doesn't exist
    """
    sql_path = get_sql_file_path(category, filename)
    
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    
    logger.debug(f"Loading SQL from: {sql_path}")
    return sql_path.read_text(encoding='utf-8')


def load_sql_statements(category: str, filename: str, separator: str = '-- @separator') -> List[str]:
    """
    Load a SQL file and split it into individual statements.
    
    Args:
        category: The SQL category folder
        filename: The SQL filename
        separator: The marker used to separate SQL statements
    
    Returns:
        List of SQL statements
    """
    content = load_sql_file(category, filename)
    
    # Split by separator and clean up
    statements = content.split(separator)
    cleaned_statements = []
    
    for stmt in statements:
        stmt = stmt.strip()
        if stmt and not stmt.startswith('--'):
            # Remove leading comment lines but keep the SQL
            lines = stmt.split('\n')
            sql_lines = []
            sql_started = False
            
            for line in lines:
                stripped = line.strip()
                if stripped and not stripped.startswith('--'):
                    sql_started = True
                if sql_started:
                    sql_lines.append(line)
            
            if sql_lines:
                cleaned_statements.append('\n'.join(sql_lines).strip())
    
    logger.debug(f"Loaded {len(cleaned_statements)} SQL statements from {filename}")
    return cleaned_statements


def load_named_sql_queries(category: str, filename: str) -> Dict[str, str]:
    """
    Load a SQL file with named queries marked by '-- @name: query_name' comments.
    
    Args:
        category: The SQL category folder
        filename: The SQL filename
    
    Returns:
        Dictionary mapping query names to SQL statements
    """
    content = load_sql_file(category, filename)
    queries = {}
    current_name = None
    current_lines = []
    
    for line in content.split('\n'):
        stripped = line.strip()
        
        # Check for name marker
        if stripped.startswith('-- @name:'):
            # Save previous query if exists
            if current_name and current_lines:
                queries[current_name] = '\n'.join(current_lines).strip()
            
            # Start new query
            current_name = stripped.replace('-- @name:', '').strip()
            current_lines = []
        elif stripped == '-- @separator':
            # Save current query at separator
            if current_name and current_lines:
                queries[current_name] = '\n'.join(current_lines).strip()
                current_name = None
                current_lines = []
        elif current_name is not None:
            # Skip other comment lines at the beginning
            if current_lines or (not stripped.startswith('--') and stripped):
                current_lines.append(line)
    
    # Don't forget the last query
    if current_name and current_lines:
        queries[current_name] = '\n'.join(current_lines).strip()
    
    logger.debug(f"Loaded {len(queries)} named queries from {filename}")
    return queries


class SQLLoader:
    """
    A class to manage loading SQL files for different database categories.
    """
    
    def __init__(self, base_path: Optional[Path] = None):
        """
        Initialize the SQL loader.
        
        Args:
            base_path: Optional custom base path for SQL files
        """
        self.base_path = base_path or SQL_BASE_PATH
    
    def staging(self, filename: str) -> str:
        """Load a SQL file from the staging category."""
        return load_sql_file('staging', filename)
    
    def analytics(self, filename: str) -> str:
        """Load a SQL file from the analytics category."""
        return load_sql_file('analytics', filename)
    
    def staging_statements(self, filename: str) -> List[str]:
        """Load and split staging SQL file into statements."""
        return load_sql_statements('staging', filename)
    
    def analytics_statements(self, filename: str) -> List[str]:
        """Load and split analytics SQL file into statements."""
        return load_sql_statements('analytics', filename)
    
    def get_staging_queries(self, filename: str) -> Dict[str, str]:
        """Load named queries from a staging SQL file."""
        return load_named_sql_queries('staging', filename)
    
    def get_analytics_queries(self, filename: str) -> Dict[str, str]:
        """Load named queries from an analytics SQL file."""
        return load_named_sql_queries('analytics', filename)


# Singleton instance for convenience
sql_loader = SQLLoader()
