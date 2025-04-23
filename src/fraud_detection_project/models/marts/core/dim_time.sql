-- models/marts/core/dim_time.sql
{{
    config(
        materialized='table',
        unique_key='time_key'
    )
}}

SELECT
    MD5(timestamp) AS time_key,
    timestamp,
    DATE(timestamp) AS date,
    EXTRACT(HOUR FROM timestamp) AS hour,
    EXTRACT(MINUTE FROM timestamp) AS minute,
    EXTRACT(SECOND FROM timestamp) AS second,
    day_of_week,
    transaction_hour,
    -- Phân loại thời gian
    CASE
        WHEN transaction_hour BETWEEN 0 AND 5 THEN 'Night'
        WHEN transaction_hour BETWEEN 6 AND 11 THEN 'Morning'
        WHEN transaction_hour BETWEEN 12 AND 17 THEN 'Afternoon'
        ELSE 'Evening'
    END AS time_of_day,
    -- Phân loại ngày trong tuần
    CASE
        WHEN day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM {{ ref('stg_processed_fraud_transactions') }}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8