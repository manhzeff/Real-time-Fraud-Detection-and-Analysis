-- models/marts/core/dim_device.sql
{{
    config(
        materialized='table',
        unique_key='device_id'
    )
}}

SELECT
    device_id,
    device_os,
    device_model,
    ip_address,
    user_agent,
    -- Phân loại thiết bị
    CASE
        WHEN device_os IN ('Android', 'iOS') THEN 'Mobile'
        WHEN device_os IN ('Windows', 'macOS', 'Linux') THEN 'Desktop'
        ELSE 'Other'
    END AS device_type
FROM {{ ref('stg_processed_fraud_transactions') }}
GROUP BY 1, 2, 3, 4, 5