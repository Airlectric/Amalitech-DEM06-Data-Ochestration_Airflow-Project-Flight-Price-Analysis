-- Upsert invalid records to quarantine table
-- @name: upsert_to_quarantine
INSERT INTO staging_db.flight_prices_quarantine (
    id, airline, source, source_name, destination, destination_name,
    departure_date_time, arrival_date_time, duration_hrs, stopovers,
    aircraft_type, class, booking_source, base_fare_bdt, tax_surcharge_bdt,
    total_fare_bdt, seasonality, days_before_departure, ingestion_timestamp,
    file_name, source_row_number, is_valid, validation_message,
    quarantine_timestamp, quarantine_reason_summary
)
SELECT 
    id, airline, source, source_name, destination, destination_name,
    departure_date_time, arrival_date_time, duration_hrs, stopovers,
    aircraft_type, class, booking_source, base_fare_bdt, tax_surcharge_bdt,
    total_fare_bdt, seasonality, days_before_departure, ingestion_timestamp,
    file_name, source_row_number, is_valid, validation_message,
    CURRENT_TIMESTAMP, LEFT(validation_message, 500)
FROM staging_db.flight_prices_raw
WHERE is_valid = FALSE
ON DUPLICATE KEY UPDATE
    airline = VALUES(airline),
    source = VALUES(source),
    source_name = VALUES(source_name),
    destination = VALUES(destination),
    destination_name = VALUES(destination_name),
    departure_date_time = VALUES(departure_date_time),
    arrival_date_time = VALUES(arrival_date_time),
    duration_hrs = VALUES(duration_hrs),
    stopovers = VALUES(stopovers),
    aircraft_type = VALUES(aircraft_type),
    class = VALUES(class),
    booking_source = VALUES(booking_source),
    base_fare_bdt = VALUES(base_fare_bdt),
    tax_surcharge_bdt = VALUES(tax_surcharge_bdt),
    total_fare_bdt = VALUES(total_fare_bdt),
    seasonality = VALUES(seasonality),
    days_before_departure = VALUES(days_before_departure),
    ingestion_timestamp = VALUES(ingestion_timestamp),
    file_name = VALUES(file_name),
    source_row_number = VALUES(source_row_number),
    is_valid = VALUES(is_valid),
    validation_message = VALUES(validation_message),
    quarantine_timestamp = CURRENT_TIMESTAMP,
    quarantine_reason_summary = VALUES(quarantine_reason_summary);

-- @separator

-- Delete invalid records from raw table after quarantine
-- @name: delete_quarantined_from_raw
DELETE FROM staging_db.flight_prices_raw
WHERE is_valid = FALSE;
