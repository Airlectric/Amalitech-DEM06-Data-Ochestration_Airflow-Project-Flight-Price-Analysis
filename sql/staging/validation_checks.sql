-- Validation Check 1: Missing required column values
-- @name: check_missing_required_values
UPDATE staging_db.flight_prices_raw
SET is_valid = FALSE,
    validation_message = CONCAT(COALESCE(validation_message, ''), '; Missing required column values')
WHERE airline IS NULL
   OR source IS NULL
   OR destination IS NULL
   OR base_fare_bdt IS NULL
   OR tax_surcharge_bdt IS NULL
   OR total_fare_bdt IS NULL;

-- @separator

-- Validation Check 2: Negative or zero fare
-- @name: check_negative_zero_fare
UPDATE staging_db.flight_prices_raw
SET is_valid = FALSE,
    validation_message = CONCAT(COALESCE(validation_message, ''), '; Negative or zero fare')
WHERE base_fare_bdt <= 0
   OR tax_surcharge_bdt < 0
   OR total_fare_bdt <= 0;

-- @separator

-- Validation Check 3: Invalid duration
-- @name: check_invalid_duration
UPDATE staging_db.flight_prices_raw
SET is_valid = FALSE,
    validation_message = CONCAT(COALESCE(validation_message, ''), '; Invalid duration')
WHERE duration_hrs <= 0 OR duration_hrs > 40;

-- @separator

-- Validation Check 4: Days before departure out of range
-- @name: check_days_before_departure
UPDATE staging_db.flight_prices_raw
SET is_valid = FALSE,
    validation_message = CONCAT(COALESCE(validation_message, ''), '; Days before departure out of range')
WHERE days_before_departure < 0 OR days_before_departure > 365;

-- @separator

-- Validation Check 5: Departure after arrival
-- @name: check_departure_after_arrival
UPDATE staging_db.flight_prices_raw
SET is_valid = FALSE,
    validation_message = CONCAT(COALESCE(validation_message, ''), '; Departure after arrival')
WHERE departure_date_time >= arrival_date_time;
