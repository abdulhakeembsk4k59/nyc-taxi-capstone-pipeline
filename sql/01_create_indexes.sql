-- 01_create_indexes.sql
-- Run after taxi_trips and taxi_zones are loaded.

CREATE INDEX IF NOT EXISTS idx_trips_pickup_datetime
    ON taxi_trips (tpep_pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_trips_pickup_date
    ON taxi_trips (pickup_date);

CREATE INDEX IF NOT EXISTS idx_trips_pickup_hour
    ON taxi_trips (pickup_hour);

CREATE INDEX IF NOT EXISTS idx_trips_pickup_week
    ON taxi_trips (pickup_week);

CREATE INDEX IF NOT EXISTS idx_trips_pu_location
    ON taxi_trips (pu_location_id);

CREATE INDEX IF NOT EXISTS idx_trips_do_location
    ON taxi_trips (do_location_id);

CREATE INDEX IF NOT EXISTS idx_trips_pickup_borough
    ON taxi_trips (pickup_borough);

CREATE INDEX IF NOT EXISTS idx_trips_dropoff_borough
    ON taxi_trips (dropoff_borough);

CREATE INDEX IF NOT EXISTS idx_trips_pickup_zone
    ON taxi_trips (pickup_zone);

CREATE INDEX IF NOT EXISTS idx_trips_payment_type
    ON taxi_trips (payment_type);

CREATE INDEX IF NOT EXISTS idx_trips_distance_bucket
    ON taxi_trips (distance_bucket);

CREATE INDEX IF NOT EXISTS idx_trips_time_of_day
    ON taxi_trips (time_of_day);

CREATE INDEX IF NOT EXISTS idx_trips_is_weekend
    ON taxi_trips (is_weekend);

CREATE INDEX IF NOT EXISTS idx_trips_date_borough
    ON taxi_trips (pickup_date, pickup_borough);

CREATE INDEX IF NOT EXISTS idx_zones_borough
    ON taxi_zones (borough);