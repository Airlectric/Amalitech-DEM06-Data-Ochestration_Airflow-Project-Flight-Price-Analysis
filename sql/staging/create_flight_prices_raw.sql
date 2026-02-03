-- Create flight_prices_raw table in MySQL staging database
CREATE TABLE IF NOT EXISTS staging_db.flight_prices_raw (
    id                    BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    file_name             VARCHAR(255)        NOT NULL,
    source_row_number     BIGINT              NOT NULL,
    
    is_valid              BOOLEAN             DEFAULT TRUE,
    validation_message    TEXT,
    
    -- This unique constraint prevents duplicate records from the same file
    UNIQUE KEY uk_file_row (file_name, source_row_number),
    
    INDEX idx_route       (source, destination),
    INDEX idx_departure   (departure_date_time),
    INDEX idx_seasonality (seasonality)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
