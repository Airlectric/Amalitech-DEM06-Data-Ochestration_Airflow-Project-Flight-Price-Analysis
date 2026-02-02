-- Create kpi_top_routes table
-- @name: create_kpi_top_routes
CREATE TABLE IF NOT EXISTS kpi_top_routes (
    source_iata           VARCHAR(10),
    destination_iata      VARCHAR(10),
    route_name            VARCHAR(100),
    booking_count         BIGINT,
    avg_total_fare_bdt    DECIMAL(12,2),
    last_updated          TIMESTAMP         DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_iata, destination_iata)
);
