-- Create kpi_avg_fare_by_airline table
-- @name: create_kpi_avg_fare_by_airline
CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
    airline               VARCHAR(100)      PRIMARY KEY,
    avg_total_fare_bdt    DECIMAL(12,2),
    record_count          BIGINT,
    last_updated          TIMESTAMP         DEFAULT CURRENT_TIMESTAMP
);
