-- Create kpi_seasonal_variation table
-- @name: create_kpi_seasonal_variation
CREATE TABLE IF NOT EXISTS kpi_seasonal_variation (
    seasonality           VARCHAR(50),
    is_peak_season        BOOLEAN,
    avg_total_fare_bdt    DECIMAL(12,2),
    record_count          BIGINT,
    last_updated          TIMESTAMP         DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (seasonality, is_peak_season)
);
