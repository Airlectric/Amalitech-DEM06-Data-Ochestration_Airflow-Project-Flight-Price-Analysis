-- Upsert Seasonal Variation KPI
-- @name: upsert_seasonal_variation
INSERT INTO kpi_seasonal_variation
    (seasonality, is_peak_season, avg_total_fare_bdt, record_count, last_updated)
SELECT 
    seasonality,
    is_peak_season,
    ROUND(AVG(total_fare_bdt), 2),
    COUNT(*),
    CURRENT_TIMESTAMP
FROM fact_flight_prices
GROUP BY seasonality, is_peak_season
ON CONFLICT (seasonality, is_peak_season) 
    DO UPDATE SET
        avg_total_fare_bdt = EXCLUDED.avg_total_fare_bdt,
        record_count = EXCLUDED.record_count,
        last_updated = EXCLUDED.last_updated;
