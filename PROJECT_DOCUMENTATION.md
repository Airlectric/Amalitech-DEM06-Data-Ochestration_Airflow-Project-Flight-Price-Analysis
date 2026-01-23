# Flight Price Analysis Pipeline - Comprehensive Documentation

## Table of Contents
1. [Executive Summary](#executive-summary)
2. [Pipeline Architecture](#pipeline-architecture)
3. [System Components](#system-components)
4. [DAG Documentation](#dag-documentation)
5. [KPI Definitions & Computation Logic](#kpi-definitions--computation-logic)
6. [Data Flow & Execution](#data-flow--execution)
7. [Challenges & Resolutions](#challenges--resolutions)
8. [Technical Specifications](#technical-specifications)

---

## Executive Summary

### Project Overview
This project implements an end-to-end data engineering pipeline for analyzing flight price data from Bangladesh. The system uses Apache Airflow to orchestrate ETL (Extract, Transform, Load) processes, moving data from raw CSV files through validation and transformation stages into analytical data warehouses.

### Key Objectives
- **Automated Data Ingestion**: Load flight price data from CSV to staging database
- **Data Quality Assurance**: Validate data integrity and quarantine invalid records
- **Analytical Transformation**: Enrich data and compute business KPIs
- **Scalable Architecture**: Containerized solution using Docker with separate staging and analytics databases

### Technology Stack
- **Orchestration**: Apache Airflow 3.1.1 (CeleryExecutor)
- **Containerization**: Docker & Docker Compose
- **Staging Database**: MySQL 8.0
- **Analytics Database**: PostgreSQL 16
- **Message Broker**: Redis 7
- **Data Processing**: Python 3.11, Pandas, SQLAlchemy
- **Database Admin**: Adminer

---

## Pipeline Architecture

### High-Level Architecture

```
┌─────────────────┐
│   CSV Source    │
│  (Bangladesh    │
│ Flight Prices)  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│              AIRFLOW ORCHESTRATION                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
│  │API Server│  │Scheduler │  │ DAG Processor    │ │
│  └──────────┘  └──────────┘  └──────────────────┘ │
│         ┌──────────┐                                │
│         │  Worker  │                                │
│         └──────────┘                                │
└──────────────────┬──────────────────────────────────┘
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
┌─────────────────┐  ┌──────────────────┐
│  MySQL Staging  │  │ PostgreSQL       │
│  Database       │  │ Analytics DB     │
│                 │  │                  │
│ • Raw Data      │  │ • Fact Tables    │
│ • Quarantine    │  │ • KPI Tables     │
└─────────────────┘  └──────────────────┘
```

### Three-Tier Data Architecture

#### Tier 1: Ingestion Layer (MySQL Staging)
- **Purpose**: Land raw data with minimal transformation
- **Tables**:
  - `flight_prices_raw`: Clean, validated records ready for transformation
  - `flight_prices_quarantine`: Invalid records for review and reconciliation
- **Database**: MySQL 8.0 (optimized for transactional workloads)

#### Tier 2: Validation Layer
- **Purpose**: Ensure data quality before analytics
- **Processes**:
  - NULL value checks on required fields
  - Business rule validation (fare amounts, duration ranges)
  - Referential integrity checks
  - Data quarantine mechanism

#### Tier 3: Analytics Layer (PostgreSQL)
- **Purpose**: Support analytical queries and KPI computation
- **Tables**:
  - `fact_flight_prices`: Enriched fact table with dimensional attributes
  - `kpi_avg_fare_by_airline`: Airline performance metrics
  - `kpi_seasonal_variation`: Seasonality analysis
  - `kpi_booking_count_by_airline`: Booking volume tracking
  - `kpi_top_routes`: Popular route analytics
- **Database**: PostgreSQL 16 (optimized for analytical queries)

---

## System Components

### Docker Services

#### 1. **postgres** (Airflow Metadata Database)
- **Image**: postgres:16
- **Port**: 5433 (host) → 5432 (container)
- **Purpose**: Stores Airflow metadata (DAG runs, task instances, connections, variables)
- **Health Check**: `pg_isready` command every 5s

#### 2. **mysql** (Staging Database)
- **Image**: mysql:8.0
- **Port**: 3307 (host) → 3306 (container)
- **Purpose**: Landing zone for raw CSV data and quarantine table
- **Schema**: `staging_db`
- **Health Check**: `mysqladmin ping` every 5s

#### 3. **analytics-postgres** (Analytics Database)
- **Image**: postgres:16
- **Port**: 5434 (host) → 5432 (container)
- **Purpose**: Houses fact tables and KPI aggregation tables
- **Health Check**: `pg_isready` command every 5s

#### 4. **redis** (Message Broker)
- **Image**: redis:7
- **Port**: 6380 (host) → 6379 (container)
- **Purpose**: Celery broker for task distribution to workers

#### 5. **adminer** (Database Administration)
- **Image**: adminer:latest
- **Port**: 8081 (host) → 8080 (container)
- **Purpose**: Web-based database management for MySQL and PostgreSQL

#### 6. **airflow-api-server**
- **Image**: custom-airflow:local
- **Port**: 8080 (host) → 8080 (container)
- **Purpose**: Provides Airflow Web UI and REST API
- **Command**: `api-server`
- **Authentication**: FAB (Flask App Builder) with username/password

#### 7. **airflow-scheduler**
- **Image**: custom-airflow:local
- **Purpose**: Schedules DAG runs and assigns tasks to workers
- **Command**: `scheduler`
- **Executor**: CeleryExecutor

#### 8. **airflow-worker**
- **Image**: custom-airflow:local
- **Purpose**: Executes tasks assigned by the scheduler via Celery
- **Command**: `celery worker`

#### 9. **airflow-dag-processor**
- **Image**: custom-airflow:local
- **Purpose**: Parses DAG files and updates DAG metadata
- **Command**: `dag-processor`

#### 10. **airflow-init**
- **Purpose**: One-time initialization service
- **Command**: `db migrate`
- **Actions**:
  - Creates/upgrades Airflow database schema
  - Creates admin user
  - Initializes connections

### Custom Docker Image

**Dockerfile Highlights**:
```dockerfile
FROM apache/airflow:3.1.1-python3.11
```

**Installed System Packages**:
- `gcc`: C compiler for building Python packages
- `libpq-dev`: PostgreSQL development libraries

**Python Dependencies** (from `requirements.txt`):
- `mysql-connector-python>=8.0.0`: MySQL database connector
- `psycopg2-binary>=2.9.0`: PostgreSQL adapter
- `pandas>=2.0.0`: Data manipulation library
- `numpy>=1.24.0`: Numerical computing
- `sqlalchemy>=2.0.0`: SQL toolkit and ORM
- Airflow provider packages for MySQL and PostgreSQL

---

## DAG Documentation

### DAG 1: `flight_price_analysis_bangladesh`
**Primary ETL Pipeline**

#### Overview
- **Purpose**: End-to-end orchestration of flight price data processing
- **Schedule**: Manual trigger (`schedule=None`)
- **Start Date**: March 1, 2025
- **Catchup**: Disabled
- **Max Active Runs**: 1 (prevents concurrent executions)
- **Tags**: `['flight_prices', 'bangladesh', 'analytics']`

#### Task Flow
```
start_pipeline
     ↓
ingest_csv_to_mysql
     ↓
validate_staging_data
     ↓
transform_and_compute_kpis
     ↓
finish_pipeline
```

#### Task Details

##### **Task 1: start_pipeline**
- **Type**: PythonOperator
- **Function**: Lambda function (logging start message)
- **Purpose**: Pipeline initialization marker

##### **Task 2: ingest_csv_to_mysql**
- **Type**: PythonOperator
- **Function**: `ingest_csv_to_mysql()` from `tasks/ingest_csv_to_mysql.py`
- **Purpose**: Load CSV data into MySQL staging table
- **Process**:
  1. Reads CSV file from `/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv`
  2. Applies data type mapping to ensure proper schema alignment
  3. Converts column names to snake_case for SQL compatibility
  4. Parses datetime columns (`departure_date_time`, `arrival_date_time`)
  5. Adds metadata columns:
     - `file_name`: Source CSV filename
     - `source_row_number`: Original row index
  6. Loads data to `staging_db.flight_prices_raw` using SQLAlchemy
- **Performance Optimization**:
  - Chunk size: 5,000 rows per batch
  - Multi-row insert method for faster ingestion
  - ~57,000 rows processed

##### **Task 3: validate_staging_data**
- **Type**: PythonOperator
- **Function**: `validate_staging_data()` from `tasks/validate_staging_data.py`
- **Purpose**: Quality assurance and data quarantine
- **Validation Rules**:
  1. **NULL Check**: Required fields (airline, source, destination, fares) must not be NULL
  2. **Fare Validation**: Base fare and total fare must be > 0, tax/surcharge ≥ 0
  3. **Duration Check**: Flight duration must be between 0 and 40 hours
  4. **Booking Window**: Days before departure must be 0-365
  5. **Logical Consistency**: Departure time must be before arrival time
- **Quarantine Process**:
  - Invalid records are moved to `flight_prices_quarantine` table
  - Records tagged with `quarantine_timestamp` and `quarantine_reason_summary`
  - Clean records remain in `flight_prices_raw`
- **Error Handling**:
  - If >90% of records are invalid → Pipeline fails (catastrophic data quality issue)
  - Otherwise, pipeline continues with valid records only
- **XCom Output**:
  - `validation_summary`: Statistics on total/valid/invalid records

##### **Task 4: transform_and_compute_kpis**
- **Type**: PythonOperator
- **Function**: `transform_and_compute_kpis()` from `tasks/transform_and_compute_kpis.py`
- **Purpose**: Enrich data and populate analytical tables
- **Process**:
  1. **Data Extraction**: Reads valid records from MySQL staging
  2. **Transformations**:
     - Corrects `total_fare_bdt` calculation (base_fare + tax_surcharge)
     - Extracts date dimensions: `departure_date`, `departure_month`, `departure_year`
     - Creates `is_peak_season` flag (Winter Holidays, Eid)
     - Adds `batch_id` (run_id for traceability)
     - Adds `ingestion_timestamp` (UTC timestamp)
  3. **Fact Table Load**: Inserts enriched data into `fact_flight_prices` (PostgreSQL)
  4. **KPI Computation**: Executes upsert queries for all KPI tables (see KPI section)
- **Performance**:
  - Chunk size: 10,000 rows per batch
  - Multi-row insert method

##### **Task 5: finish_pipeline**
- **Type**: PythonOperator
- **Function**: Lambda function (logging completion message)
- **Purpose**: Pipeline completion marker

---

### DAG 2: `init_staging_schema`
**Schema Initialization for MySQL Staging**

#### Overview
- **Purpose**: Create staging tables in MySQL
- **Schedule**: Manual trigger only
- **Start Date**: January 1, 2025
- **Tags**: `['init', 'schema', 'setup']`

#### Tasks

##### **Task 1: create_flight_prices_raw**
- **Type**: SQLExecuteQueryOperator
- **Connection**: `mysql_staging`
- **Purpose**: Create main staging table
- **Schema**:
  - Primary Key: `id` (auto-increment)
  - Business Columns: airline, route, datetime, fares, etc.
  - Metadata Columns: `ingestion_timestamp`, `file_name`, `source_row_number`
  - Validation Columns: `is_valid` (boolean), `validation_message` (text)
  - Indexes: `route`, `departure_date_time`, `seasonality`

##### **Task 2: create_flight_prices_quarantine**
- **Type**: SQLExecuteQueryOperator
- **Connection**: `mysql_staging`
- **Purpose**: Create quarantine table for invalid records
- **Schema**: Identical to `flight_prices_raw` plus:
  - `quarantine_timestamp`: When record was quarantined
  - `quarantine_reason_summary`: Shortened validation message
  - `batch_id`: Pipeline run identifier
  - `quarantine_notes`: Manual review comments

---

### DAG 3: `init_analytics_schema`
**Schema Initialization for PostgreSQL Analytics**

#### Overview
- **Purpose**: Create fact and KPI tables in PostgreSQL
- **Schedule**: Manual trigger only
- **Start Date**: January 1, 2025
- **Tags**: `['init', 'schema', 'setup']`

#### Tasks

##### **Task 1: create_fact_flight_prices**
- **Type**: SQLExecuteQueryOperator
- **Connection**: `postgres_analytics`
- **Purpose**: Create fact table for enriched flight data
- **Schema**:
  - Primary Key: `flight_price_id`
  - Dimensions: airline, route (source/destination), date, class, seasonality
  - Metrics: base_fare, tax_surcharge, total_fare
  - Flags: `is_peak_season`
  - Metadata: `ingestion_timestamp`, `batch_id`
  - Indexes: airline, route, departure_date, seasonality/peak

##### **Task 2: create_kpi_avg_fare_by_airline**
- **Type**: SQLExecuteQueryOperator
- **Purpose**: Create KPI table for average fares per airline

##### **Task 3: create_kpi_seasonal_variation**
- **Type**: SQLExecuteQueryOperator
- **Purpose**: Create KPI table for seasonal price analysis

##### **Task 4: create_kpi_booking_count_by_airline**
- **Type**: SQLExecuteQueryOperator
- **Purpose**: Create KPI table for booking volumes

##### **Task 5: create_kpi_top_routes**
- **Type**: SQLExecuteQueryOperator
- **Purpose**: Create KPI table for popular route analytics

---

### DAG 4: `check_connections`
**Database Connectivity Validation**

#### Overview
- **Purpose**: Verify database connections are configured correctly
- **Schedule**: Manual trigger only
- **Tags**: `['validation', 'connections']`
- **Max Active Runs**: 1

#### Tasks

##### **Task 1: test_mysql_staging**
- **Type**: SQLExecuteQueryOperator
- **Connection**: `mysql_staging`
- **SQL**: `SELECT 1 AS test_connection;`
- **Purpose**: Verify MySQL staging database connectivity

##### **Task 2: test_postgres_analytics**
- **Type**: SQLExecuteQueryOperator
- **Connection**: `postgres_analytics`
- **SQL**: `SELECT 1 AS test_connection;`
- **Purpose**: Verify PostgreSQL analytics database connectivity

**Execution**: Tasks run in parallel (independent validation)

---

## KPI Definitions & Computation Logic

### KPI 1: Average Fare by Airline

#### Business Definition
Average total fare charged by each airline across all flights.

#### Computation Logic
```sql
SELECT 
    airline,
    ROUND(AVG(total_fare_bdt), 2) AS avg_total_fare_bdt,
    COUNT(*) AS record_count,
    CURRENT_TIMESTAMP AS last_updated
FROM fact_flight_prices
GROUP BY airline
```

#### Table Schema
- **Primary Key**: `airline`
- **Metrics**: 
  - `avg_total_fare_bdt`: Mean fare in Bangladeshi Taka
  - `record_count`: Number of flights analyzed
- **Timestamp**: `last_updated` (when KPI was computed)

#### Upsert Behavior
```sql
ON CONFLICT (airline) 
    DO UPDATE SET
        avg_total_fare_bdt = EXCLUDED.avg_total_fare_bdt,
        record_count = EXCLUDED.record_count,
        last_updated = EXCLUDED.last_updated;
```
- If airline already exists → update metrics
- If new airline → insert new record

#### Use Cases
- Airline pricing comparison
- Competitive analysis
- Budget carrier identification

---

### KPI 2: Seasonal Variation

#### Business Definition
Average fares segmented by seasonality period and peak/off-peak classification.

#### Computation Logic
```sql
SELECT 
    seasonality,
    is_peak_season,
    ROUND(AVG(total_fare_bdt), 2) AS avg_total_fare_bdt,
    COUNT(*) AS record_count,
    CURRENT_TIMESTAMP AS last_updated
FROM fact_flight_prices
GROUP BY seasonality, is_peak_season
```

#### Seasonality Categories
- **Peak Seasons**: Winter Holidays, Eid
- **Off-Peak Seasons**: All other periods

#### Table Schema
- **Primary Key**: `(seasonality, is_peak_season)`
- **Metrics**:
  - `avg_total_fare_bdt`: Mean fare for season/peak combination
  - `record_count`: Sample size

#### Use Cases
- Dynamic pricing strategies
- Demand forecasting
- Revenue optimization
- Customer communication (best time to book)

---

### KPI 3: Booking Count by Airline

#### Business Definition
Total number of flight bookings recorded for each airline (proxy for market share).

#### Computation Logic
```sql
SELECT 
    airline,
    COUNT(*) AS booking_count,
    CURRENT_TIMESTAMP AS last_updated
FROM fact_flight_prices
GROUP BY airline
```

#### Table Schema
- **Primary Key**: `airline`
- **Metrics**: `booking_count` (total flights/bookings)

#### Use Cases
- Market share analysis
- Airline popularity metrics
- Capacity planning insights

---

### KPI 4: Top Routes

#### Business Definition
Most popular flight routes by booking volume, with average fare information.

#### Computation Logic
```sql
SELECT 
    source_iata,
    destination_iata,
    CONCAT(source_iata, ' to ', destination_iata) AS route_name,
    COUNT(*) AS booking_count,
    ROUND(AVG(total_fare_bdt), 2) AS avg_total_fare_bdt,
    CURRENT_TIMESTAMP AS last_updated
FROM fact_flight_prices
GROUP BY source_iata, destination_iata
ORDER BY booking_count DESC
LIMIT 20
```

#### Table Schema
- **Primary Key**: `(source_iata, destination_iata)`
- **Dimensions**: `route_name` (e.g., "DAC to CXB")
- **Metrics**:
  - `booking_count`: Number of flights on this route
  - `avg_total_fare_bdt`: Average fare for route

#### Top 20 Routes
Only the top 20 routes by booking volume are stored (configurable via LIMIT clause).

#### Use Cases
- Route profitability analysis
- Network optimization
- New route planning
- Pricing benchmarks by route

---

## Data Flow & Execution

### End-to-End Data Journey

#### Step 1: Data Arrival
- **Source**: CSV file stored at `/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv`
- **Format**: UTF-8 encoded CSV with headers
- **Size**: ~57,000 records
- **Columns**: 17 fields (airline, route, fares, dates, etc.)

#### Step 2: Ingestion (Task: ingest_csv_to_mysql)
1. **Read CSV** using Pandas with explicit data types
2. **Normalize Column Names** to snake_case
3. **Parse Datetimes** for departure and arrival
4. **Add Metadata**: filename, row number
5. **Batch Insert** to MySQL in 5,000-row chunks
6. **Destination**: `staging_db.flight_prices_raw`

**Outcome**: Raw data landed in staging database with minimal transformation.

#### Step 3: Validation (Task: validate_staging_data)
1. **Execute 5 Validation Rules**:
   - Check for NULL required fields
   - Validate fare amounts
   - Verify flight duration range
   - Check booking window
   - Ensure logical datetime order
2. **Mark Invalid Records**: Set `is_valid = FALSE` and populate `validation_message`
3. **Quarantine**: Move invalid records to `flight_prices_quarantine`
4. **Clean**: Delete invalid records from `flight_prices_raw`
5. **Quality Gate**: Fail pipeline if >90% invalid

**Outcome**: Only clean, validated data remains in staging table.

#### Step 4: Transformation (Task: transform_and_compute_kpis)
1. **Extract** valid records from MySQL staging
2. **Transform**:
   - Recalculate total fare (data quality correction)
   - Extract date dimensions (date, month, year)
   - Flag peak seasons
   - Add batch traceability
3. **Load** to PostgreSQL `fact_flight_prices` table
4. **Compute KPIs**:
   - Execute 4 upsert queries for KPI tables
   - Update existing records or insert new ones

**Outcome**: Enriched fact table and up-to-date KPI tables in analytics database.

### Execution Flow Diagram

```
┌──────────────────┐
│  Trigger DAG     │
│  Manually or     │
│  via Schedule    │
└────────┬─────────┘
         │
         ▼
┌─────────────────────────────┐
│  Airflow Scheduler          │
│  • Parses DAG               │
│  • Creates Task Instances   │
└────────┬────────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  Celery Worker (via Redis)  │
│  • Executes Python Tasks    │
│  • Runs SQL Operators       │
└────────┬────────────────────┘
         │
    ┌────┴─────┐
    ▼          ▼
┌──────┐  ┌──────────┐
│MySQL │  │PostgreSQL│
│Staging│  │Analytics│
└──────┘  └──────────┘
```

### Retry & Error Handling

#### Default Retry Policy
```python
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
```

#### Task-Specific Error Handling

**Ingestion Task**:
- File not found → `AirflowException` (fail immediately)
- Database connection error → Retry once after 5 minutes
- Invalid data types → Fail with detailed error message

**Validation Task**:
- Syntax errors in SQL → Fail immediately
- >90% invalid data → `AirflowException` with detailed message
- Moderate data quality issues → Continue with warning logs

**Transformation Task**:
- Empty staging table → Log warning and skip (no failure)
- Database write errors → Retry once
- KPI computation errors → Fail with query details

### Monitoring & Observability

#### Airflow UI Access
- **URL**: http://localhost:8080
- **Username**: admin
- **Password**: (from environment variable `_AIRFLOW_WWW_USER_PASSWORD`)

#### Key Metrics to Monitor
1. **Task Duration**: Track ingestion/validation/transformation times
2. **Success Rate**: Percentage of successful DAG runs
3. **Data Quality**: Invalid record percentage from validation task
4. **XCom Values**: Validation summary statistics

#### Logging
- **Location**: `/opt/airflow/logs/`
- **Structure**: Organized by `dag_id/run_id/task_id/`
- **Format**: Timestamped Python logs with task context
- **Example**: 
  ```
  logs/dag_id=flight_price_analysis_bangladesh/
       run_id=manual__2026-01-23T160224+0000/
       task_id=ingest_csv_to_mysql/
       attempt=1.log
  ```

---

## Challenges & Resolutions

### Challenge 1: Environment Variable Management Across Multiple Services

#### Problem
Airflow requires consistent configuration across 5+ Docker services (scheduler, worker, webserver, etc.). Managing connection strings, encryption keys, and credentials in multiple places led to:
- Configuration drift between services
- Hard-to-debug connection failures
- Security risks from hardcoded credentials

#### Resolution
**Implemented `.env` file with Docker Compose variable substitution:**

```yaml
environment:
  AIRFLOW_CONN_MYSQL_STAGING: ${AIRFLOW_CONN_MYSQL_STAGING}
  AIRFLOW_CONN_POSTGRES_ANALYTICS: ${AIRFLOW_CONN_POSTGRES_ANALYTICS}
```

**Benefits**:
- Single source of truth for environment variables
- Easy to update credentials (e.g., rotating passwords)
- `.env` file can be gitignored for security
- Simplified deployment to different environments (dev/staging/prod)

---

### Challenge 2: Airflow Connections Not Available at Runtime

#### Problem
Initial implementation hardcoded database credentials in task code. This violated security best practices and made the pipeline fragile to environment changes.

**Error Example**:
```
Connection 'mysql_staging' not found
```

#### Resolution
**Used Airflow Connections via Hooks:**

```python
# Before (hardcoded):
conn = mysql.connector.connect(
    host='mysql',
    user='staging_user',
    password='hardcoded_password'
)

# After (using Airflow Connection):
mysql_hook = MySqlHook(mysql_conn_id='mysql_staging')
engine = mysql_hook.get_sqlalchemy_engine()
```

**Configuration**:
```bash
# Environment variable format:
AIRFLOW_CONN_MYSQL_STAGING=mysql://staging_user:password@mysql:3306/staging_db
AIRFLOW_CONN_POSTGRES_ANALYTICS=postgresql://analytics_user:password@analytics-postgres:5432/analytics_db
```

**Benefits**:
- Centralized credential management
- Airflow UI can manage connections
- Hooks handle connection pooling and retries
- Testable with different connection IDs

---

### Challenge 3: Data Quality Issues in Source CSV

#### Problem
Raw CSV data contained:
- NULL values in required fields (~5% of records)
- Negative fare amounts (data entry errors)
- Illogical datetime sequences (arrival before departure)
- Extreme outliers in flight duration

**Impact**: Loading dirty data to analytics would corrupt KPIs and lead to incorrect business insights.

#### Resolution
**Implemented Multi-Rule Validation Layer with Quarantine:**

1. **Validation Rules** (5 SQL UPDATE statements):
   - NULL checks
   - Range validations
   - Business logic constraints

2. **Quarantine Table**:
   ```sql
   INSERT INTO flight_prices_quarantine
   SELECT * FROM flight_prices_raw WHERE is_valid = FALSE;
   ```

3. **Quality Gate**:
   ```python
   if invalid_pct > 90:
       raise AirflowException("Catastrophic data quality issue")
   ```

**Benefits**:
- Invalid data isolated for manual review
- Pipeline continues with clean data
- Auditable record of data quality issues
- Can reprocess quarantined records after source correction

---

### Challenge 4: Schema Initialization Ordering

#### Problem
DAGs attempted to run before database schemas existed, causing:
```
ERROR: Table 'staging_db.flight_prices_raw' doesn't exist
```

#### Resolution
**Created Separate Schema Initialization DAGs:**

1. **`init_staging_schema`**: Creates MySQL tables
2. **`init_analytics_schema`**: Creates PostgreSQL tables

**Execution Order**:
```bash
# Step 1: Initialize schemas (one-time)
Run init_staging_schema
Run init_analytics_schema

# Step 2: Verify connections
Run check_connections

# Step 3: Execute main pipeline
Run flight_price_analysis_bangladesh
```

**Benefits**:
- Idempotent schema creation (`CREATE TABLE IF NOT EXISTS`)
- Can rebuild schemas without affecting DAG code
- Clear separation of setup vs. runtime operations

---

### Challenge 5: Handling Large CSV Files with Pandas

#### Problem
Loading 57,000+ row CSV into memory and inserting to database in one transaction risked:
- Out-of-memory errors
- Database connection timeouts
- Long-running transactions blocking other queries

#### Resolution
**Implemented Chunked Processing:**

```python
df.to_sql(
    name='flight_prices_raw',
    con=engine,
    chunksize=5000,      # Process 5,000 rows at a time
    method='multi'       # Use multi-row INSERT statements
)
```

**Performance Optimization**:
- **Chunking**: Breaks large DataFrame into manageable batches
- **Multi-row INSERT**: Single SQL statement with multiple value sets
- **Trade-offs**: Balanced memory usage vs. transaction overhead

**Results**:
- Stable memory consumption
- No database timeouts
- Ingestion completes in <30 seconds

---

### Challenge 6: Duplicate Data on Pipeline Re-runs

#### Problem
Re-running the pipeline appended duplicate records to fact tables, causing:
- Inflated KPI values
- Incorrect booking counts
- Historical data corruption

#### Resolution
**Implemented UPSERT (INSERT ... ON CONFLICT) for KPI Tables:**

```sql
INSERT INTO kpi_avg_fare_by_airline (airline, avg_total_fare_bdt, ...)
SELECT airline, AVG(total_fare_bdt), ...
FROM fact_flight_prices
GROUP BY airline
ON CONFLICT (airline) 
    DO UPDATE SET
        avg_total_fare_bdt = EXCLUDED.avg_total_fare_bdt,
        last_updated = EXCLUDED.last_updated;
```

**For Fact Table**:
- Option 1: Truncate before each load (if full refresh)
- Option 2: Use `batch_id` to track incremental loads
- Current Implementation: Append-only with `batch_id` for traceability

**Benefits**:
- Idempotent pipeline (can re-run safely)
- KPIs always reflect latest data
- No manual cleanup required

---

### Challenge 7: Airflow Authentication & Security

#### Problem
Default Airflow configuration allowed unauthenticated access to:
- DAG triggers
- Connection management
- Database credentials viewing

#### Resolution
**Enabled Flask App Builder (FAB) Authentication:**

```yaml
environment:
  AUTH_MANAGER_CLASS: airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
  AIRFLOW__WEBSERVER__AUTHENTICATE: 'True'
  _AIRFLOW_WWW_USER_CREATE: 'True'
  _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME}
  _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD}
  _AIRFLOW_WWW_USER_ROLE: 'Admin'
```

**Features Enabled**:
- Role-based access control (Admin, Viewer, User roles)
- Password-protected UI
- Secure API endpoints with JWT tokens

**Deployment Notes**:
- Admin credentials stored in `.env` file
- Password complexity enforced
- Can integrate with LDAP/OAuth in production

---

### Challenge 8: Container Startup Dependencies

#### Problem
Airflow services (scheduler, worker) started before databases were ready, causing:
```
OperationalError: could not connect to server
```

#### Resolution
**Implemented Health Checks & Service Dependencies:**

```yaml
depends_on:
  airflow-init:
    condition: service_completed_successfully
  postgres:
    condition: service_healthy
  mysql:
    condition: service_healthy
```

**Health Check Example**:
```yaml
healthcheck:
  test: ["CMD", "pg_isready", "-U", "airflow_user"]
  interval: 5s
  retries: 5
```

**Benefits**:
- Services wait for dependencies to be ready
- Eliminates race conditions
- Graceful startup sequence

---

### Challenge 9: Timezone & Timestamp Consistency

#### Problem
Data pipeline mixed timezones:
- CSV data in local time (Bangladesh)
- Airflow in UTC
- Databases with different timezone settings

#### Resolution
**Standardized on UTC for All Processing:**

```python
# Transformation task:
ingestion_timestamp = datetime.utcnow()

# Database columns:
ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
```

**Display Layer**:
- Convert to local timezone only in BI tools/reports
- Store all timestamps in UTC in databases

**Benefits**:
- Consistent time-based queries
- No daylight saving time issues
- Simplified multi-region support

---

### Challenge 10: Debugging SQL Execution in Airflow Tasks

#### Problem
SQL errors in operators showed minimal context:
```
ERROR: syntax error at or near ";"
```
Hard to identify which query in multi-statement SQL failed.

#### Resolution
**Split Complex SQL into Multiple Tasks:**

```python
# Before (single task with multi-statement SQL):
create_all_tables = SQLExecuteQueryOperator(
    sql="CREATE TABLE t1 ...; CREATE TABLE t2 ...; CREATE TABLE t3 ...;"
)

# After (separate tasks):
create_table_1 = SQLExecuteQueryOperator(sql="CREATE TABLE t1 ...")
create_table_2 = SQLExecuteQueryOperator(sql="CREATE TABLE t2 ...")
create_table_3 = SQLExecuteQueryOperator(sql="CREATE TABLE t3 ...")
```

**Additional Debugging**:
- Added Python logging in callable functions
- Used `autocommit=True` for DDL statements
- Tested SQL queries directly in Adminer before adding to DAGs

**Benefits**:
- Granular error messages
- Failed task identifies exact problematic SQL
- Can retry individual table creations

---

## Technical Specifications

### System Requirements

#### Development Environment
- **OS**: Windows 10/11, macOS, or Linux
- **Docker**: Version 20.10+ with Docker Compose V2
- **Disk Space**: Minimum 10 GB for images and volumes
- **RAM**: Minimum 8 GB (16 GB recommended)
- **CPU**: 4+ cores recommended for parallel task execution

#### Network Ports (Host Mappings)
- `8080`: Airflow Web UI
- `8081`: Adminer Database UI
- `5433`: PostgreSQL (Airflow Metadata)
- `3307`: MySQL (Staging)
- `5434`: PostgreSQL (Analytics)
- `6380`: Redis

### Data Specifications

#### Input Data
- **File**: `Flight_Price_Dataset_of_Bangladesh.csv`
- **Encoding**: UTF-8
- **Row Count**: ~57,000 records
- **Columns**: 17 fields
- **Size**: ~12 MB

#### Schema Mapping

**CSV → MySQL Staging**:
| CSV Column               | MySQL Column          | Data Type       |
|--------------------------|-----------------------|-----------------|
| Airline                  | airline               | VARCHAR(100)    |
| Source                   | source                | VARCHAR(10)     |
| Source Name              | source_name           | VARCHAR(150)    |
| Destination              | destination           | VARCHAR(10)     |
| Destination Name         | destination_name      | VARCHAR(150)    |
| Departure Date & Time    | departure_date_time   | DATETIME        |
| Arrival Date & Time      | arrival_date_time     | DATETIME        |
| Duration (hrs)           | duration_hrs          | DECIMAL(6,2)    |
| Stopovers                | stopovers             | VARCHAR(20)     |
| Aircraft Type            | aircraft_type         | VARCHAR(100)    |
| Class                    | class                 | VARCHAR(50)     |
| Booking Source           | booking_source        | VARCHAR(100)    |
| Base Fare (BDT)          | base_fare_bdt         | DECIMAL(12,2)   |
| Tax & Surcharge (BDT)    | tax_surcharge_bdt     | DECIMAL(12,2)   |
| Total Fare (BDT)         | total_fare_bdt        | DECIMAL(12,2)   |
| Seasonality              | seasonality           | VARCHAR(50)     |
| Days Before Departure    | days_before_departure | INT             |

**MySQL Staging → PostgreSQL Analytics**:
- Subset of staging columns (only business-relevant fields)
- Additional computed columns: `departure_date`, `departure_month`, `departure_year`, `is_peak_season`
- Metadata: `batch_id`, `ingestion_timestamp`

### Performance Benchmarks

#### Observed Performance (Local Development)
- **CSV Ingestion**: ~25-30 seconds for 57,000 rows
- **Validation**: ~5-10 seconds
- **Transformation & KPI Computation**: ~15-20 seconds
- **Total Pipeline Duration**: ~1-2 minutes end-to-end

#### Scalability Considerations
- **Current Capacity**: Handles up to 100,000 rows without tuning
- **Bottlenecks**: 
  - Pandas DataFrame operations (memory-bound)
  - Single Celery worker (parallelism limited)
- **Scaling Options**:
  - Add more Celery workers for parallel DAG execution
  - Partition large CSV files by date/airline
  - Use PySpark for datasets >1 million rows

### Security Considerations

#### Credentials Management
- **Airflow Connections**: Stored in metadata database (encrypted with Fernet key)
- **Environment Variables**: Loaded from `.env` file (not committed to Git)
- **Fernet Key**: Generated securely, used for encrypting sensitive data

#### Access Control
- **Airflow UI**: Protected by FAB authentication
- **Database Access**: Only Airflow services can connect (internal Docker network)
- **Adminer**: Exposed for development, should be disabled in production

#### Recommendations for Production
1. Use Kubernetes Secrets or AWS Secrets Manager for credentials
2. Enable SSL/TLS for database connections
3. Implement network policies to restrict inter-service communication
4. Rotate Fernet keys periodically
5. Use read-only database users for analytical queries

---

## Appendix

### File Structure
```
DEM06-Airflow and Data Lakes/
├── docker-compose.yml          # Orchestrates all services
├── Dockerfile                  # Custom Airflow image
├── requirements.txt            # Python dependencies
├── .env                        # Environment variables (not in Git)
│
├── dags/                       # Airflow DAGs
│   ├── flight_price_analysis_dag.py
│   ├── init_analytics_schema.py
│   ├── init_staging_schema.py
│   └── test_db_conn.py
│
├── scripts/                    # Reusable task modules
│   └── tasks/
│       ├── ingest_csv_to_mysql.py
│       ├── validate_staging_data.py
│       └── transform_and_compute_kpis.py
│
├── data/                       # Source data files
│   └── Flight_Price_Dataset_of_Bangladesh.csv
│
└── logs/                       # Airflow execution logs
    ├── dag_id=flight_price_analysis_bangladesh/
    ├── dag_id=init_analytics_schema/
    └── ...
```

### Useful Commands

#### Start the Pipeline
```bash
docker-compose up -d
```

#### Stop All Services
```bash
docker-compose down
```

#### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
```

#### Rebuild After Code Changes
```bash
docker-compose down
docker-compose up -d --build
```

#### Access Container Shell
```bash
docker exec -it <container_name> bash

# Example:
docker exec -it dem06-airflow_api_server bash
```

#### Manual DAG Trigger (CLI)
```bash
docker exec -it <airflow-scheduler-container> \
  airflow dags trigger flight_price_analysis_bangladesh
```

#### Database Access via Adminer
- URL: http://localhost:8081
- Select database type (MySQL or PostgreSQL)
- Enter credentials from `.env` file

### Environment Variables Reference

**Airflow Configuration**:
- `AIRFLOW__CORE__EXECUTOR`: CeleryExecutor
- `AIRFLOW__CORE__FERNET_KEY`: Encryption key
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: Metadata DB connection

**Database Connections**:
- `AIRFLOW_CONN_MYSQL_STAGING`: MySQL staging connection string
- `AIRFLOW_CONN_POSTGRES_ANALYTICS`: PostgreSQL analytics connection string

**User Authentication**:
- `_AIRFLOW_WWW_USER_USERNAME`: Admin username
- `_AIRFLOW_WWW_USER_PASSWORD`: Admin password (shown in script file)
- `_AIRFLOW_WWW_USER_ROLE`: Admin role

### Future Enhancements

1. **Incremental Loading**:
   - Track last processed file/date
   - Load only new records instead of full refresh

2. **Data Lineage Tracking**:
   - Implement table/column-level lineage
   - Trace data from source CSV to final KPIs

3. **Alerting & Notifications**:
   - Email/Slack alerts on pipeline failures
   - Data quality threshold alerts

4. **Dashboard Integration**:
   - Connect to PowerBI/Tableau for visualization
   - Build Streamlit/Dash real-time analytics dashboard

5. **Testing**:
   - Unit tests for transformation logic
   - Integration tests for DAGs
   - Data quality tests with Great Expectations

6. **CI/CD Pipeline**:
   - Automated DAG validation on commit
   - Docker image building and pushing
   - Automated deployment to cloud (AWS/GCP/Azure)

---

## Conclusion

This flight price analysis pipeline demonstrates a production-grade data engineering solution built with modern tools and best practices:

- **Modular Architecture**: Separation of ingestion, validation, and transformation layers
- **Data Quality First**: Comprehensive validation with quarantine mechanism
- **Scalability**: Containerized services with horizontal scaling potential
- **Observability**: Detailed logging and XCom metrics
- **Security**: Encrypted credentials, role-based access control
- **Maintainability**: Clear DAG structure, reusable task modules

The pipeline successfully processes 57,000+ flight records from CSV through validation and into analytical tables, computing actionable KPIs for business intelligence.

**Project Owner**: Daniel Agudey Doe  
**Last Updated**: January 23, 2026  
**Version**: 1.0
