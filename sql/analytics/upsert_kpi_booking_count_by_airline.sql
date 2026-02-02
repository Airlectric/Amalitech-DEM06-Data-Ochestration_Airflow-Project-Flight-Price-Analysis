-- Upsert Booking Count by Airline KPI
-- @name: upsert_booking_count_by_airline
INSERT INTO kpi_booking_count_by_airline
    (airline, booking_count, last_updated)
SELECT 
    airline,
    COUNT(*),
    CURRENT_TIMESTAMP
FROM fact_flight_prices
GROUP BY airline
ON CONFLICT (airline) 
    DO UPDATE SET
        booking_count = EXCLUDED.booking_count,
        last_updated = EXCLUDED.last_updated;
