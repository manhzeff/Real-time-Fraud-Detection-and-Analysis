{{
    config(
        materialized='view'
    )
}}

SELECT
    transaction_id,
    amount,
    currency,
    timestamp,
    customer_id,
    customer_name,
    customer_email,
    customer_age,
    customer_segment,
    location,
    is_fraud,
    transaction_type,
    device_id,
    device_os,
    device_model,
    ip_address,
    merchant_id,
    merchant_name,
    merchant_category,
    user_agent,
    payment_method,
    transaction_status,
    latitude,
    longitude,
    account_balance,
    transaction_hour,
    day_of_week,
    risk_score,
    fraud_score,
    loyalty_points,
    fraud_rules,
    MD5(payment_method) AS payment_method_key,
    MD5(location || CAST(latitude AS VARCHAR) || CAST(longitude AS VARCHAR)) AS location_key,
    MD5(timestamp) AS time_key
FROM {{ source('snowflake', 'PROCESSED_FRAUD_TRANSACTIONS') }}