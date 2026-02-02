-- Create kpi_booking_count_by_airline table
-- @name: create_kpi_booking_count_by_airline
CREATE TABLE IF NOT EXISTS kpi_booking_count_by_airline (
    airline               VARCHAR(100)      PRIMARY KEY,
    booking_count         BIGINT,
    last_updated          TIMESTAMP         DEFAULT CURRENT_TIMESTAMP
);
