CREATE OR REPLACE VIEW vw_daily_summary AS
SELECT
    pickup_date,
    pickup_day_of_week,
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_revenue,
    ROUND(SUM(tip_amount)::NUMERIC, 2) AS total_tips,
    ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_fare,
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_mins)::NUMERIC, 2) AS avg_duration_mins,
    ROUND(AVG(tip_percentage)::NUMERIC, 2) AS avg_tip_pct,
    SUM(passenger_count) AS total_passengers,
    ROUND(
        (SUM(total_amount) - LAG(SUM(total_amount))
            OVER (ORDER BY pickup_date))::NUMERIC,
        2
    ) AS revenue_vs_prev_day,
    ROUND(
        AVG(SUM(total_amount))
            OVER (ORDER BY pickup_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::NUMERIC,
        2
    ) AS revenue_7day_rolling_avg
FROM taxi_trips
GROUP BY pickup_date, pickup_day_of_week, is_weekend
ORDER BY pickup_date;



CREATE OR REPLACE VIEW vw_zone_performance AS
SELECT
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    COUNT(*) AS total_pickups,
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_fare,
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance,
    ROUND(AVG(trip_duration_mins)::NUMERIC, 2) AS avg_duration_mins,
    ROUND(AVG(tip_percentage)::NUMERIC, 2) AS avg_tip_pct,
    RANK() OVER (
        ORDER BY SUM(total_amount) DESC
    ) AS global_revenue_rank,
    RANK() OVER (
        PARTITION BY pickup_borough
        ORDER BY SUM(total_amount) DESC
    ) AS borough_revenue_rank
FROM taxi_trips
WHERE pickup_zone IS NOT NULL
GROUP BY pickup_borough, pickup_zone, pickup_service_zone;


CREATE OR REPLACE VIEW vw_hourly_demand AS
SELECT
    pickup_hour,
    time_of_day,
    COUNT(*) AS total_trips,
    ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_fare,
    ROUND(SUM(total_amount)::NUMERIC, 2) AS total_revenue,
    ROUND(AVG(trip_duration_mins)::NUMERIC, 2) AS avg_duration_mins,
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS pct_of_all_trips,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS demand_rank
FROM taxi_trips
GROUP BY pickup_hour, time_of_day
ORDER BY pickup_hour;


CREATE OR REPLACE VIEW vw_route_analysis AS
SELECT
    pickup_zone AS from_zone,
    dropoff_zone AS to_zone,
    pickup_borough AS from_borough,
    dropoff_borough AS to_borough,
    CONCAT(pickup_zone, ' -> ', dropoff_zone) AS route_label,
    COUNT(*) AS total_trips,
    ROUND(AVG(total_amount)::NUMERIC, 2) AS avg_fare,
    ROUND(AVG(trip_distance)::NUMERIC, 2) AS avg_distance,
    ROUND(AVG(trip_duration_mins)::NUMERIC, 2) AS avg_duration_mins,
    ROUND(AVG(tip_percentage)::NUMERIC, 2) AS avg_tip_pct,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS popularity_rank
FROM taxi_trips
WHERE pickup_zone IS NOT NULL
  AND dropoff_zone IS NOT NULL
GROUP BY pickup_zone, dropoff_zone, pickup_borough, dropoff_borough
ORDER BY total_trips DESC;