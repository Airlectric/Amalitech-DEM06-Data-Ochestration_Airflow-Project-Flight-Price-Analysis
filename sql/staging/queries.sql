-- Count total rows in raw table
-- @name: count_total_rows
SELECT COUNT(*) FROM staging_db.flight_prices_raw;

-- @separator

-- Count invalid rows in raw table
-- @name: count_invalid_rows
SELECT COUNT(*) FROM staging_db.flight_prices_raw WHERE is_valid = FALSE;

-- @separator

-- Get affected row count from last operation
-- @name: get_row_count
SELECT ROW_COUNT();

-- @separator

-- Select valid records for transformation
-- @name: select_valid_records
SELECT 
    id AS flight_price_id,
    airline,
    source AS source_iata,
    destination AS destination_iata,
    departure_date_time,
    class,
    seasonality,
    days_before_departure,
    base_fare_bdt,
    tax_surcharge_bdt,
    total_fare_bdt
FROM staging_db.flight_prices_raw
WHERE is_valid = TRUE;
