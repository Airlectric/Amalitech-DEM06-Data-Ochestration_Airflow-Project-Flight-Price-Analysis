-- Create flight_prices_quarantine table in MySQL staging database
CREATE TABLE IF NOT EXISTS staging_db.flight_prices_quarantine (
    -- Same structure as raw table + quarantine-specific columns
    id                    BIGINT,
    airline               VARCHAR(100)        NOT NULL,
    source                VARCHAR(10)         NOT NULL,
    source_name           VARCHAR(150),
    destination           VARCHAR(10)         NOT NULL,
    destination_name      VARCHAR(150),
    departure_date_time   DATETIME            NOT NULL,
    arrival_date_time     DATETIME            NOT NULL,
    duration_hrs          DECIMAL(6,2)        NOT NULL,
    stopovers             VARCHAR(20)         NOT NULL,
    aircraft_type         VARCHAR(100),
    class                 VARCHAR(50)         NOT NULL,
    booking_source        VARCHAR(100),
    base_fare_bdt         DECIMAL(12,2)       NOT NULL,
    tax_surcharge_bdt     DECIMAL(12,2)       NOT NULL,
    total_fare_bdt        DECIMAL(12,2)       NOT NULL,
    seasonality           VARCHAR(50)         NOT NULL,
    days_before_departure INT                 NOT NULL,
    
    ingestion_timestamp   TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,
    file_name             VARCHAR(255),
    source_row_number     BIGINT,
    
    is_valid              BOOLEAN             DEFAULT FALSE,   -- always false here
    validation_message    TEXT,
    
    -- Quarantine metadata
    quarantine_timestamp       TIMESTAMP           DEFAULT CURRENT_TIMESTAMP,
    quarantine_reason_summary  VARCHAR(500),   -- shortened version of validation_message
    batch_id                   VARCHAR(50),    -- optional: run_id or date
    quarantine_notes           TEXT,           -- for manual comments later
    
    PRIMARY KEY (id),
    INDEX idx_quarantine_ts (quarantine_timestamp),
    INDEX idx_reason        (quarantine_reason_summary(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
