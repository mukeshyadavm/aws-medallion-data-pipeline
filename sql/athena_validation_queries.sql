USE nyc_taxi_db;

-- Glue table names observed in nyc_taxi_db:
-- bronze, silver_silver,
-- gold_daily_revenue, gold_passenger_analysis, gold_payment_type_analysis, gold_vendor_revenue,
-- star_dim_date, star_dim_ingestion_batch, star_dim_location, star_dim_passenger_count,
-- star_dim_payment_type, star_dim_rate_code, star_dim_time, star_dim_trip_flags,
-- star_dim_trip_quality, star_dim_vendor,
-- star_fact_daily_trip_summary, star_fact_location_daily_summary,
-- star_fact_payment_daily_summary, star_fact_trip, star_fact_vendor_daily_summary.

-- 1. Row counts across source, curated, legacy gold, and star-schema tables.
SELECT 'bronze' AS table_name, COUNT(*) AS row_count FROM bronze
UNION ALL SELECT 'silver_silver', COUNT(*) FROM silver_silver
UNION ALL SELECT 'gold_daily_revenue', COUNT(*) FROM gold_daily_revenue
UNION ALL SELECT 'gold_passenger_analysis', COUNT(*) FROM gold_passenger_analysis
UNION ALL SELECT 'gold_payment_type_analysis', COUNT(*) FROM gold_payment_type_analysis
UNION ALL SELECT 'gold_vendor_revenue', COUNT(*) FROM gold_vendor_revenue
UNION ALL SELECT 'star_fact_trip', COUNT(*) FROM star_fact_trip
UNION ALL SELECT 'star_fact_daily_trip_summary', COUNT(*) FROM star_fact_daily_trip_summary
UNION ALL SELECT 'star_fact_vendor_daily_summary', COUNT(*) FROM star_fact_vendor_daily_summary
UNION ALL SELECT 'star_fact_payment_daily_summary', COUNT(*) FROM star_fact_payment_daily_summary
UNION ALL SELECT 'star_fact_location_daily_summary', COUNT(*) FROM star_fact_location_daily_summary
UNION ALL SELECT 'star_dim_date', COUNT(*) FROM star_dim_date
UNION ALL SELECT 'star_dim_time', COUNT(*) FROM star_dim_time
UNION ALL SELECT 'star_dim_vendor', COUNT(*) FROM star_dim_vendor
UNION ALL SELECT 'star_dim_payment_type', COUNT(*) FROM star_dim_payment_type
UNION ALL SELECT 'star_dim_rate_code', COUNT(*) FROM star_dim_rate_code
UNION ALL SELECT 'star_dim_location', COUNT(*) FROM star_dim_location
UNION ALL SELECT 'star_dim_passenger_count', COUNT(*) FROM star_dim_passenger_count
UNION ALL SELECT 'star_dim_trip_flags', COUNT(*) FROM star_dim_trip_flags
UNION ALL SELECT 'star_dim_trip_quality', COUNT(*) FROM star_dim_trip_quality
UNION ALL SELECT 'star_dim_ingestion_batch', COUNT(*) FROM star_dim_ingestion_batch;

-- 2. Silver-to-fact row-count reconciliation for valid records.
WITH silver_valid AS (
    SELECT COUNT(*) AS valid_silver_rows
    FROM silver_silver
    WHERE tpep_pickup_datetime IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL
      AND tpep_dropoff_datetime >= tpep_pickup_datetime
      AND trip_distance >= 0
      AND total_amount >= 0
),
fact_rows AS (
    SELECT COUNT(*) AS fact_rows
    FROM star_fact_trip
)
SELECT
    valid_silver_rows,
    fact_rows,
    fact_rows - valid_silver_rows AS difference
FROM silver_valid CROSS JOIN fact_rows;

-- 3. Revenue totals: valid Silver vs atomic fact.
WITH silver_revenue AS (
    SELECT ROUND(SUM(total_amount), 2) AS silver_total_revenue
    FROM silver_silver
    WHERE tpep_pickup_datetime IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL
      AND tpep_dropoff_datetime >= tpep_pickup_datetime
      AND trip_distance >= 0
      AND total_amount >= 0
),
fact_revenue AS (
    SELECT ROUND(SUM(total_amount), 2) AS fact_total_revenue
    FROM star_fact_trip
)
SELECT
    silver_total_revenue,
    fact_total_revenue,
    ROUND(fact_total_revenue - silver_total_revenue, 2) AS difference
FROM silver_revenue CROSS JOIN fact_revenue;

-- 4. Revenue totals: atomic fact vs star aggregate facts.
WITH fact_total AS (
    SELECT ROUND(SUM(total_amount), 2) AS total_revenue
    FROM star_fact_trip
),
daily_total AS (
    SELECT ROUND(SUM(total_revenue), 2) AS total_revenue
    FROM star_fact_daily_trip_summary
),
vendor_total AS (
    SELECT ROUND(SUM(total_revenue), 2) AS total_revenue
    FROM star_fact_vendor_daily_summary
),
payment_total AS (
    SELECT ROUND(SUM(total_revenue), 2) AS total_revenue
    FROM star_fact_payment_daily_summary
),
location_total AS (
    SELECT ROUND(SUM(total_revenue), 2) AS total_revenue
    FROM star_fact_location_daily_summary
)
SELECT 'star_fact_daily_trip_summary' AS aggregate_table, f.total_revenue AS fact_total_revenue, d.total_revenue AS aggregate_total_revenue, ROUND(d.total_revenue - f.total_revenue, 2) AS difference
FROM fact_total f CROSS JOIN daily_total d
UNION ALL
SELECT 'star_fact_vendor_daily_summary', f.total_revenue, v.total_revenue, ROUND(v.total_revenue - f.total_revenue, 2)
FROM fact_total f CROSS JOIN vendor_total v
UNION ALL
SELECT 'star_fact_payment_daily_summary', f.total_revenue, p.total_revenue, ROUND(p.total_revenue - f.total_revenue, 2)
FROM fact_total f CROSS JOIN payment_total p
UNION ALL
SELECT 'star_fact_location_daily_summary', f.total_revenue, l.total_revenue, ROUND(l.total_revenue - f.total_revenue, 2)
FROM fact_total f CROSS JOIN location_total l;

-- 5. Duplicate trip_id check.
SELECT trip_id, COUNT(*) AS duplicate_count
FROM star_fact_trip
GROUP BY trip_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20;

-- 6. Null primary-key checks.
SELECT 'star_fact_trip.trip_id' AS primary_key, COUNT(*) AS null_count FROM star_fact_trip WHERE trip_id IS NULL
UNION ALL SELECT 'star_fact_daily_trip_summary.pickup_date_key', COUNT(*) FROM star_fact_daily_trip_summary WHERE pickup_date_key IS NULL
UNION ALL SELECT 'star_fact_vendor_daily_summary.pickup_date_key', COUNT(*) FROM star_fact_vendor_daily_summary WHERE pickup_date_key IS NULL
UNION ALL SELECT 'star_fact_vendor_daily_summary.vendor_key', COUNT(*) FROM star_fact_vendor_daily_summary WHERE vendor_key IS NULL
UNION ALL SELECT 'star_fact_payment_daily_summary.pickup_date_key', COUNT(*) FROM star_fact_payment_daily_summary WHERE pickup_date_key IS NULL
UNION ALL SELECT 'star_fact_payment_daily_summary.payment_type_key', COUNT(*) FROM star_fact_payment_daily_summary WHERE payment_type_key IS NULL
UNION ALL SELECT 'star_fact_location_daily_summary.pickup_date_key', COUNT(*) FROM star_fact_location_daily_summary WHERE pickup_date_key IS NULL
UNION ALL SELECT 'star_fact_location_daily_summary.pu_location_key', COUNT(*) FROM star_fact_location_daily_summary WHERE pu_location_key IS NULL
UNION ALL SELECT 'star_fact_location_daily_summary.do_location_key', COUNT(*) FROM star_fact_location_daily_summary WHERE do_location_key IS NULL
UNION ALL SELECT 'star_dim_date.date_key', COUNT(*) FROM star_dim_date WHERE date_key IS NULL
UNION ALL SELECT 'star_dim_time.time_key', COUNT(*) FROM star_dim_time WHERE time_key IS NULL
UNION ALL SELECT 'star_dim_vendor.vendor_key', COUNT(*) FROM star_dim_vendor WHERE vendor_key IS NULL
UNION ALL SELECT 'star_dim_payment_type.payment_type_key', COUNT(*) FROM star_dim_payment_type WHERE payment_type_key IS NULL
UNION ALL SELECT 'star_dim_rate_code.rate_code_key', COUNT(*) FROM star_dim_rate_code WHERE rate_code_key IS NULL
UNION ALL SELECT 'star_dim_location.location_key', COUNT(*) FROM star_dim_location WHERE location_key IS NULL
UNION ALL SELECT 'star_dim_passenger_count.passenger_count_key', COUNT(*) FROM star_dim_passenger_count WHERE passenger_count_key IS NULL
UNION ALL SELECT 'star_dim_trip_flags.trip_flags_key', COUNT(*) FROM star_dim_trip_flags WHERE trip_flags_key IS NULL
UNION ALL SELECT 'star_dim_trip_quality.trip_quality_key', COUNT(*) FROM star_dim_trip_quality WHERE trip_quality_key IS NULL
UNION ALL SELECT 'star_dim_ingestion_batch.ingestion_batch_key', COUNT(*) FROM star_dim_ingestion_batch WHERE ingestion_batch_key IS NULL;

-- 7. Null foreign-key checks on the atomic fact.
SELECT
    SUM(CASE WHEN ingestion_batch_key IS NULL THEN 1 ELSE 0 END) AS null_ingestion_batch_key,
    SUM(CASE WHEN pickup_date_key IS NULL THEN 1 ELSE 0 END) AS null_pickup_date_key,
    SUM(CASE WHEN dropoff_date_key IS NULL THEN 1 ELSE 0 END) AS null_dropoff_date_key,
    SUM(CASE WHEN pickup_time_key IS NULL THEN 1 ELSE 0 END) AS null_pickup_time_key,
    SUM(CASE WHEN dropoff_time_key IS NULL THEN 1 ELSE 0 END) AS null_dropoff_time_key,
    SUM(CASE WHEN vendor_key IS NULL THEN 1 ELSE 0 END) AS null_vendor_key,
    SUM(CASE WHEN payment_type_key IS NULL THEN 1 ELSE 0 END) AS null_payment_type_key,
    SUM(CASE WHEN rate_code_key IS NULL THEN 1 ELSE 0 END) AS null_rate_code_key,
    SUM(CASE WHEN pu_location_key IS NULL THEN 1 ELSE 0 END) AS null_pu_location_key,
    SUM(CASE WHEN do_location_key IS NULL THEN 1 ELSE 0 END) AS null_do_location_key,
    SUM(CASE WHEN passenger_count_key IS NULL THEN 1 ELSE 0 END) AS null_passenger_count_key,
    SUM(CASE WHEN trip_flags_key IS NULL THEN 1 ELSE 0 END) AS null_trip_flags_key,
    SUM(CASE WHEN trip_quality_key IS NULL THEN 1 ELSE 0 END) AS null_trip_quality_key
FROM star_fact_trip;

-- 8. Foreign-key mismatch checks on the atomic fact.
SELECT 'pickup_date_key -> star_dim_date' AS relationship_name, COUNT(*) AS missing_rows
FROM star_fact_trip f LEFT JOIN star_dim_date d ON f.pickup_date_key = d.date_key
WHERE f.pickup_date_key IS NOT NULL AND d.date_key IS NULL
UNION ALL
SELECT 'dropoff_date_key -> star_dim_date', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_date d ON f.dropoff_date_key = d.date_key
WHERE f.dropoff_date_key IS NOT NULL AND d.date_key IS NULL
UNION ALL
SELECT 'pickup_time_key -> star_dim_time', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_time d ON f.pickup_time_key = d.time_key
WHERE f.pickup_time_key IS NOT NULL AND d.time_key IS NULL
UNION ALL
SELECT 'dropoff_time_key -> star_dim_time', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_time d ON f.dropoff_time_key = d.time_key
WHERE f.dropoff_time_key IS NOT NULL AND d.time_key IS NULL
UNION ALL
SELECT 'vendor_key -> star_dim_vendor', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_vendor d ON f.vendor_key = d.vendor_key
WHERE f.vendor_key IS NOT NULL AND d.vendor_key IS NULL
UNION ALL
SELECT 'payment_type_key -> star_dim_payment_type', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_payment_type d ON f.payment_type_key = d.payment_type_key
WHERE f.payment_type_key IS NOT NULL AND d.payment_type_key IS NULL
UNION ALL
SELECT 'rate_code_key -> star_dim_rate_code', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_rate_code d ON f.rate_code_key = d.rate_code_key
WHERE f.rate_code_key IS NOT NULL AND d.rate_code_key IS NULL
UNION ALL
SELECT 'pu_location_key -> star_dim_location', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_location d ON f.pu_location_key = d.location_key
WHERE f.pu_location_key IS NOT NULL AND d.location_key IS NULL
UNION ALL
SELECT 'do_location_key -> star_dim_location', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_location d ON f.do_location_key = d.location_key
WHERE f.do_location_key IS NOT NULL AND d.location_key IS NULL
UNION ALL
SELECT 'passenger_count_key -> star_dim_passenger_count', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_passenger_count d ON f.passenger_count_key = d.passenger_count_key
WHERE f.passenger_count_key IS NOT NULL AND d.passenger_count_key IS NULL
UNION ALL
SELECT 'trip_flags_key -> star_dim_trip_flags', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_trip_flags d ON f.trip_flags_key = d.trip_flags_key
WHERE f.trip_flags_key IS NOT NULL AND d.trip_flags_key IS NULL
UNION ALL
SELECT 'trip_quality_key -> star_dim_trip_quality', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_trip_quality d ON f.trip_quality_key = d.trip_quality_key
WHERE f.trip_quality_key IS NOT NULL AND d.trip_quality_key IS NULL
UNION ALL
SELECT 'ingestion_batch_key -> star_dim_ingestion_batch', COUNT(*)
FROM star_fact_trip f LEFT JOIN star_dim_ingestion_batch d ON f.ingestion_batch_key = d.ingestion_batch_key
WHERE f.ingestion_batch_key IS NOT NULL AND d.ingestion_batch_key IS NULL;

-- 9. Foreign-key mismatch checks on star aggregate facts.
SELECT 'star_fact_daily_trip_summary.pickup_date_key -> star_dim_date' AS relationship_name, COUNT(*) AS missing_rows
FROM star_fact_daily_trip_summary f LEFT JOIN star_dim_date d ON f.pickup_date_key = d.date_key
WHERE f.pickup_date_key IS NOT NULL AND d.date_key IS NULL
UNION ALL
SELECT 'star_fact_vendor_daily_summary.pickup_date_key -> star_dim_date', COUNT(*)
FROM star_fact_vendor_daily_summary f LEFT JOIN star_dim_date d ON f.pickup_date_key = d.date_key
WHERE f.pickup_date_key IS NOT NULL AND d.date_key IS NULL
UNION ALL
SELECT 'star_fact_vendor_daily_summary.vendor_key -> star_dim_vendor', COUNT(*)
FROM star_fact_vendor_daily_summary f LEFT JOIN star_dim_vendor d ON f.vendor_key = d.vendor_key
WHERE f.vendor_key IS NOT NULL AND d.vendor_key IS NULL
UNION ALL
SELECT 'star_fact_payment_daily_summary.pickup_date_key -> star_dim_date', COUNT(*)
FROM star_fact_payment_daily_summary f LEFT JOIN star_dim_date d ON f.pickup_date_key = d.date_key
WHERE f.pickup_date_key IS NOT NULL AND d.date_key IS NULL
UNION ALL
SELECT 'star_fact_payment_daily_summary.payment_type_key -> star_dim_payment_type', COUNT(*)
FROM star_fact_payment_daily_summary f LEFT JOIN star_dim_payment_type d ON f.payment_type_key = d.payment_type_key
WHERE f.payment_type_key IS NOT NULL AND d.payment_type_key IS NULL
UNION ALL
SELECT 'star_fact_location_daily_summary.pickup_date_key -> star_dim_date', COUNT(*)
FROM star_fact_location_daily_summary f LEFT JOIN star_dim_date d ON f.pickup_date_key = d.date_key
WHERE f.pickup_date_key IS NOT NULL AND d.date_key IS NULL
UNION ALL
SELECT 'star_fact_location_daily_summary.pu_location_key -> star_dim_location', COUNT(*)
FROM star_fact_location_daily_summary f LEFT JOIN star_dim_location d ON f.pu_location_key = d.location_key
WHERE f.pu_location_key IS NOT NULL AND d.location_key IS NULL
UNION ALL
SELECT 'star_fact_location_daily_summary.do_location_key -> star_dim_location', COUNT(*)
FROM star_fact_location_daily_summary f LEFT JOIN star_dim_location d ON f.do_location_key = d.location_key
WHERE f.do_location_key IS NOT NULL AND d.location_key IS NULL;

-- 10. Date dimension join validation.
SELECT 'pickup_date_key' AS date_join, COUNT(*) AS joined_rows, COUNT_IF(d.date_key IS NULL) AS missing_date_rows
FROM star_fact_trip f LEFT JOIN star_dim_date d ON f.pickup_date_key = d.date_key
UNION ALL
SELECT 'dropoff_date_key', COUNT(*), COUNT_IF(d.date_key IS NULL)
FROM star_fact_trip f LEFT JOIN star_dim_date d ON f.dropoff_date_key = d.date_key
UNION ALL
SELECT 'daily_summary_pickup_date_key', COUNT(*), COUNT_IF(d.date_key IS NULL)
FROM star_fact_daily_trip_summary f LEFT JOIN star_dim_date d ON f.pickup_date_key = d.date_key
UNION ALL
SELECT 'vendor_summary_pickup_date_key', COUNT(*), COUNT_IF(d.date_key IS NULL)
FROM star_fact_vendor_daily_summary f LEFT JOIN star_dim_date d ON f.pickup_date_key = d.date_key
UNION ALL
SELECT 'payment_summary_pickup_date_key', COUNT(*), COUNT_IF(d.date_key IS NULL)
FROM star_fact_payment_daily_summary f LEFT JOIN star_dim_date d ON f.pickup_date_key = d.date_key
UNION ALL
SELECT 'location_summary_pickup_date_key', COUNT(*), COUNT_IF(d.date_key IS NULL)
FROM star_fact_location_daily_summary f LEFT JOIN star_dim_date d ON f.pickup_date_key = d.date_key;

-- 11. Unknown dimension member checks.
SELECT 'star_dim_date' AS dimension_name, COUNT(*) AS unknown_member_count FROM star_dim_date WHERE date_key = -1
UNION ALL SELECT 'star_dim_time', COUNT(*) FROM star_dim_time WHERE time_key = -1
UNION ALL SELECT 'star_dim_vendor', COUNT(*) FROM star_dim_vendor WHERE vendor_key = -1
UNION ALL SELECT 'star_dim_payment_type', COUNT(*) FROM star_dim_payment_type WHERE payment_type_key = -1
UNION ALL SELECT 'star_dim_rate_code', COUNT(*) FROM star_dim_rate_code WHERE rate_code_key = -1
UNION ALL SELECT 'star_dim_location', COUNT(*) FROM star_dim_location WHERE location_key = -1
UNION ALL SELECT 'star_dim_passenger_count', COUNT(*) FROM star_dim_passenger_count WHERE passenger_count_key = -1
UNION ALL SELECT 'star_dim_trip_flags', COUNT(*) FROM star_dim_trip_flags WHERE trip_flags_key = -1
UNION ALL SELECT 'star_dim_trip_quality', COUNT(*) FROM star_dim_trip_quality WHERE trip_quality_key = -1;

-- 12. Dimension uniqueness checks.
SELECT 'star_dim_date' AS table_name, COUNT(*) AS rows, COUNT(DISTINCT date_key) AS distinct_keys FROM star_dim_date
UNION ALL SELECT 'star_dim_time', COUNT(*), COUNT(DISTINCT time_key) FROM star_dim_time
UNION ALL SELECT 'star_dim_vendor', COUNT(*), COUNT(DISTINCT vendor_key) FROM star_dim_vendor
UNION ALL SELECT 'star_dim_payment_type', COUNT(*), COUNT(DISTINCT payment_type_key) FROM star_dim_payment_type
UNION ALL SELECT 'star_dim_rate_code', COUNT(*), COUNT(DISTINCT rate_code_key) FROM star_dim_rate_code
UNION ALL SELECT 'star_dim_location', COUNT(*), COUNT(DISTINCT location_key) FROM star_dim_location
UNION ALL SELECT 'star_dim_passenger_count', COUNT(*), COUNT(DISTINCT passenger_count_key) FROM star_dim_passenger_count
UNION ALL SELECT 'star_dim_trip_flags', COUNT(*), COUNT(DISTINCT trip_flags_key) FROM star_dim_trip_flags
UNION ALL SELECT 'star_dim_trip_quality', COUNT(*), COUNT(DISTINCT trip_quality_key) FROM star_dim_trip_quality
UNION ALL SELECT 'star_dim_ingestion_batch', COUNT(*), COUNT(DISTINCT ingestion_batch_key) FROM star_dim_ingestion_batch;

-- 13. Partition visibility.
SHOW PARTITIONS star_fact_trip;
SHOW PARTITIONS star_fact_daily_trip_summary;
SHOW PARTITIONS star_fact_vendor_daily_summary;
SHOW PARTITIONS star_fact_payment_daily_summary;
SHOW PARTITIONS star_fact_location_daily_summary;

-- 14. Quality distribution.
SELECT
    q.quality_code,
    q.quality_description,
    COUNT(*) AS total_trips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM star_fact_trip f
JOIN star_dim_trip_quality q ON f.trip_quality_key = q.trip_quality_key
GROUP BY q.quality_code, q.quality_description
ORDER BY total_trips DESC;

-- 15. Batch audit.
SELECT
    batch_id,
    job_name,
    source_table,
    source_record_count,
    valid_record_count,
    rejected_record_count,
    target_base_path
FROM star_dim_ingestion_batch
ORDER BY ingestion_batch_key DESC;

-- 16. Core business validation: daily revenue.
SELECT
    d.calendar_date,
    COUNT(*) AS total_trips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_revenue,
    ROUND(AVG(f.trip_distance), 2) AS avg_trip_distance
FROM star_fact_trip f
JOIN star_dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.calendar_date
ORDER BY d.calendar_date;

-- 17. Vendor performance.
SELECT
    v.vendor_name,
    COUNT(*) AS total_trips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(SUM(f.total_amount) / NULLIF(SUM(f.trip_distance), 0), 2) AS revenue_per_mile
FROM star_fact_trip f
JOIN star_dim_vendor v ON f.vendor_key = v.vendor_key
GROUP BY v.vendor_name
ORDER BY total_revenue DESC;

-- 18. Payment and tip behavior.
SELECT
    p.payment_type_description,
    COUNT(*) AS total_trips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(SUM(f.tip_amount), 2) AS total_tips,
    ROUND(SUM(f.tip_amount) / NULLIF(SUM(f.fare_amount), 0), 4) AS tip_pct
FROM star_fact_trip f
JOIN star_dim_payment_type p ON f.payment_type_key = p.payment_type_key
GROUP BY p.payment_type_description
ORDER BY total_revenue DESC;
