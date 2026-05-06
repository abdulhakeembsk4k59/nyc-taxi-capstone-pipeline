-- ============================================================
-- 02_analytics_queries.sql
-- NYC Taxi Capstone - Business Analytics Queries
-- ============================================================
-- Purpose: Answer 8 real business questions using SQL.
-- Run these manually in pgAdmin after the pipeline has loaded.
-- ============================================================



-- ============================================================
-- QUERY 1: Revenue by Borough
-- Question: Which boroughs generate the most taxi revenue?
-- Technique: GROUP BY, aggregate functions, ORDER BY
-- ============================================================
SELECT
    pickup_borough,
    COUNT(*)                                            AS total_trips,
    ROUND(SUM(total_amount)::NUMERIC, 2)                AS total_revenue,
    ROUND(AVG(total_amount)::NUMERIC, 2)                AS avg_fare,
    ROUND(AVG(tip_percentage)::NUMERIC, 2)              AS avg_tip_pct,
    ROUND(AVG(trip_distance)::NUMERIC, 2)               AS avg_distance_miles,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2
    )                                                   AS pct_of_all_trips
FROM taxi_trips
WHERE pickup_borough IS NOT NULL
GROUP BY pickup_borough
ORDER BY total_revenue DESC;



-- ============================================================
-- QUERY 2: Hourly Demand Pattern
-- Question: What hours of the day are busiest and most profitable?
-- Technique: GROUP BY hour, window function for percentage share
-- ============================================================

SELECT
    pickup_hour,
    time_of_day,
    COUNT(*)                                            AS total_trips,
    ROUND(SUM(total_amount)::NUMERIC, 2)                AS total_revenue,
    ROUND(AVG(total_amount)::NUMERIC, 2)                AS avg_fare,
    ROUND(AVG(trip_duration_mins)::NUMERIC, 2)          AS avg_duration_mins,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2
    )                                                   AS pct_of_daily_trips
FROM taxi_trips
GROUP BY pickup_hour, time_of_day
ORDER BY pickup_hour;


-- ============================================================
-- QUERY 3: Average Fare by Distance Bucket
-- Question: How does trip length affect fare and tip behavior?
-- Technique: GROUP BY engineered bucket
-- ============================================================
SELECT
    distance_bucket,
    COUNT(*)                                            AS total_trips,
    ROUND(AVG(fare_amount)::NUMERIC, 2)                 AS avg_fare,
    ROUND(AVG(total_amount)::NUMERIC, 2)                AS avg_total,
    ROUND(AVG(fare_per_mile)::NUMERIC, 2)               AS avg_fare_per_mile,
    ROUND(AVG(tip_percentage)::NUMERIC, 2)              AS avg_tip_pct,
    ROUND(AVG(trip_duration_mins)::NUMERIC, 2)          AS avg_duration_mins,
    ROUND(SUM(total_amount)::NUMERIC, 2)                AS total_revenue
FROM taxi_trips
WHERE distance_bucket IS NOT NULL
  AND distance_bucket != 'nan'
GROUP BY distance_bucket
ORDER BY avg_fare ASC;



-- ============================================================
-- QUERY 4: Top 10 Busiest Routes
-- Question: Which pickup -> dropoff zone pairs are most common?
-- Technique: Multi-column GROUP BY, LIMIT
-- ============================================================
SELECT
    pickup_zone                                         AS from_zone,
    dropoff_zone                                        AS to_zone,
    pickup_borough                                      AS from_borough,
    dropoff_borough                                     AS to_borough,
    COUNT(*)                                            AS total_trips,
    ROUND(AVG(total_amount)::NUMERIC, 2)                AS avg_fare,
    ROUND(AVG(trip_duration_mins)::NUMERIC, 2)          AS avg_duration_mins,
    ROUND(AVG(tip_percentage)::NUMERIC, 2)              AS avg_tip_pct
FROM taxi_trips
WHERE pickup_zone  IS NOT NULL
  AND dropoff_zone IS NOT NULL
GROUP BY pickup_zone, dropoff_zone, pickup_borough, dropoff_borough
ORDER BY total_trips DESC
LIMIT 10;



-- ============================================================
-- QUERY 5: Daily Revenue Trend (January 2024)
-- Question: How does revenue and demand evolve over the month?
-- Technique: GROUP BY date, cumulative window, LAG
-- ============================================================
SELECT
    pickup_date,
    COUNT(*)                                            AS daily_trips,
    ROUND(SUM(total_amount)::NUMERIC, 2)                AS daily_revenue,
    ROUND(AVG(total_amount)::NUMERIC, 2)                AS avg_fare,
    ROUND(AVG(trip_distance)::NUMERIC, 2)               AS avg_distance,
    SUM(COUNT(*))
        OVER (ORDER BY pickup_date)                     AS cumulative_trips,
    ROUND(
        SUM(SUM(total_amount)) OVER (ORDER BY pickup_date)::NUMERIC, 2
    )                                                   AS cumulative_revenue,
    ROUND(
        (SUM(total_amount) - LAG(SUM(total_amount))
            OVER (ORDER BY pickup_date))::NUMERIC, 2
    )                                                   AS revenue_change_vs_prev_day
FROM taxi_trips
GROUP BY pickup_date
ORDER BY pickup_date;



-- ============================================================
-- QUERY 6: Payment Method Analysis
-- Question: How do payment methods affect tip behavior?
-- Technique: GROUP BY decoded payment type, window for share
-- ============================================================
SELECT
    payment_type_desc,
    COUNT(*)                                            AS total_trips,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_trips,
    ROUND(AVG(tip_amount)::NUMERIC, 2)                  AS avg_tip_amount,
    ROUND(AVG(tip_percentage)::NUMERIC, 2)              AS avg_tip_pct,
    ROUND(SUM(total_amount)::NUMERIC, 2)                AS total_revenue,
    ROUND(AVG(total_amount)::NUMERIC, 2)                AS avg_fare
FROM taxi_trips
GROUP BY payment_type_desc
ORDER BY total_trips DESC;



-- ============================================================
-- QUERY 7: Weekend vs Weekday Performance
-- Question: Do weekend or weekday trips perform better?
-- Technique: CASE WHEN, GROUP BY boolean
-- ============================================================
SELECT
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END  AS day_type,
    COUNT(*)                                                AS total_trips,
    ROUND(SUM(total_amount)::NUMERIC, 2)                    AS total_revenue,
    ROUND(AVG(total_amount)::NUMERIC, 2)                    AS avg_fare,
    ROUND(AVG(trip_distance)::NUMERIC, 2)                   AS avg_distance,
    ROUND(AVG(tip_percentage)::NUMERIC, 2)                  AS avg_tip_pct,
    ROUND(AVG(trip_duration_mins)::NUMERIC, 2)              AS avg_duration_mins
FROM taxi_trips
GROUP BY is_weekend
ORDER BY day_type;


-- ============================================================
-- QUERY 8: Top 5 Zones per Borough - Revenue Ranking
-- Question: Within each borough, which zones earn the most?
-- Technique: Subquery + RANK() with PARTITION BY
-- ============================================================
SELECT
    pickup_borough,
    pickup_zone,
    total_trips,
    total_revenue,
    avg_fare,
    zone_revenue_rank
FROM (
    SELECT
        pickup_borough,
        pickup_zone,
        COUNT(*)                                        AS total_trips,
        ROUND(SUM(total_amount)::NUMERIC, 2)            AS total_revenue,
        ROUND(AVG(total_amount)::NUMERIC, 2)            AS avg_fare,
        RANK() OVER (
            PARTITION BY pickup_borough
            ORDER BY SUM(total_amount) DESC
        )                                               AS zone_revenue_rank
    FROM taxi_trips
    WHERE pickup_zone IS NOT NULL
    GROUP BY pickup_borough, pickup_zone
) ranked
WHERE zone_revenue_rank <= 5
ORDER BY pickup_borough, zone_revenue_rank;