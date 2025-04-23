-- models/marts/core/dim_location.sql
{{
    config(
        materialized='table',
        unique_key='location_key'
    )
}}

SELECT
    MD5(location || CAST(latitude AS VARCHAR) || CAST(longitude AS VARCHAR)) AS location_key,
    location AS country_code,
    latitude,
    longitude,
    -- Có thể thêm các phân loại khu vực khác nếu cần
    CASE
        WHEN location IN ('US', 'CA') THEN 'North America'
        WHEN location IN ('UK', 'DE', 'FR') THEN 'Europe'
        WHEN location IN ('JP', 'SG', 'VN') THEN 'Asia'
        WHEN location = 'AU' THEN 'Oceania'
        ELSE 'Other'
    END AS continent
FROM {{ ref('stg_processed_fraud_transactions') }}
GROUP BY 1, 2, 3, 4