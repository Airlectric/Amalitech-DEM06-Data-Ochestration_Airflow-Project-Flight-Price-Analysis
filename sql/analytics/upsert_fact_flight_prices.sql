-- Batch upsert fact_flight_prices data (used with execute_values)
-- @name: batch_upsert_fact_flight_prices
INSERT INTO fact_flight_prices (
    flight_price_id, airline, source_iata, destination_iata,
    departure_date, departure_month, departure_year, class,
    seasonality, is_peak_season, days_before_departure,
    base_fare_bdt, tax_surcharge_bdt, total_fare_bdt,
    ingestion_timestamp, batch_id
) VALUES %s
ON CONFLICT (flight_price_id) 
    DO UPDATE SET
        airline = EXCLUDED.airline,
        source_iata = EXCLUDED.source_iata,
        destination_iata = EXCLUDED.destination_iata,
        departure_date = EXCLUDED.departure_date,
        departure_month = EXCLUDED.departure_month,
        departure_year = EXCLUDED.departure_year,
        class = EXCLUDED.class,
        seasonality = EXCLUDED.seasonality,
        is_peak_season = EXCLUDED.is_peak_season,
        days_before_departure = EXCLUDED.days_before_departure,
        base_fare_bdt = EXCLUDED.base_fare_bdt,
        tax_surcharge_bdt = EXCLUDED.tax_surcharge_bdt,
        total_fare_bdt = EXCLUDED.total_fare_bdt,
        ingestion_timestamp = EXCLUDED.ingestion_timestamp,
        batch_id = EXCLUDED.batch_id;
