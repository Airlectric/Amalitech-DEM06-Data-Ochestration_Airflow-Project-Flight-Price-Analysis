-- Create fact_flight_prices table in PostgreSQL analytics database
-- @name: create_fact_flight_prices
CREATE TABLE IF NOT EXISTS fact_flight_prices (
    flight_price_id       BIGINT PRIMARY KEY,
    airline               VARCHAR(100)      NOT NULL,
    source_iata           VARCHAR(10)       NOT NULL,
    destination_iata      VARCHAR(10)       NOT NULL,
    departure_date        DATE              NOT NULL,
    departure_month       INT               NOT NULL,
    departure_year        INT               NOT NULL,
    class                 VARCHAR(50)       NOT NULL,
    seasonality           VARCHAR(50)       NOT NULL,
    is_peak_season        BOOLEAN           NOT NULL,
    days_before_departure INT               NOT NULL,
    base_fare_bdt         DECIMAL(12,2)     NOT NULL,
    tax_surcharge_bdt     DECIMAL(12,2)     NOT NULL,
    total_fare_bdt        DECIMAL(12,2)     NOT NULL,
    ingestion_timestamp   TIMESTAMP         DEFAULT CURRENT_TIMESTAMP,
    batch_id              VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_airline ON fact_flight_prices (airline);
CREATE INDEX IF NOT EXISTS idx_route ON fact_flight_prices (source_iata, destination_iata);
CREATE INDEX IF NOT EXISTS idx_departure_date ON fact_flight_prices (departure_date);
CREATE INDEX IF NOT EXISTS idx_seasonality ON fact_flight_prices (seasonality, is_peak_season);
