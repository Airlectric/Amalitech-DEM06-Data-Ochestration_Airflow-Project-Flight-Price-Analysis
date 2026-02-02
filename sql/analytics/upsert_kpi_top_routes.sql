-- Upsert Top Routes KPI
-- @name: upsert_top_routes
INSERT INTO kpi_top_routes
    (source_iata, destination_iata, route_name, booking_count, avg_total_fare_bdt, last_updated)
SELECT 
    source_iata,
    destination_iata,
    CONCAT(source_iata, ' to ', destination_iata) AS route_name,
    COUNT(*) AS booking_count,
    ROUND(AVG(total_fare_bdt), 2) AS avg_total_fare_bdt,
    CURRENT_TIMESTAMP
FROM fact_flight_prices
GROUP BY source_iata, destination_iata
ON CONFLICT (source_iata, destination_iata) 
    DO UPDATE SET
        booking_count = EXCLUDED.booking_count,
        avg_total_fare_bdt = EXCLUDED.avg_total_fare_bdt,
        route_name = EXCLUDED.route_name,
        last_updated = EXCLUDED.last_updated;
