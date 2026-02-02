-- Upsert Average Fare by Airline KPI
-- @name: upsert_avg_fare_by_airline
INSERT INTO kpi_avg_fare_by_airline
    (airline, avg_total_fare_bdt, record_count, last_updated)
SELECT 
    airline,
    ROUND(AVG(total_fare_bdt), 2),
    COUNT(*),
    CURRENT_TIMESTAMP
FROM fact_flight_prices
GROUP BY airline
ON CONFLICT (airline) 
    DO UPDATE SET
        avg_total_fare_bdt = EXCLUDED.avg_total_fare_bdt,
        record_count = EXCLUDED.record_count,
        last_updated = EXCLUDED.last_updated;
