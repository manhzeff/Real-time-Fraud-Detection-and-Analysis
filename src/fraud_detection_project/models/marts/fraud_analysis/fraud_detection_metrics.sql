{{
    config(
        materialized='table'
    )
}}

SELECT
    dm.merchant_category,
    dm.risk_category,
    dl.country_code,
    dl.continent,
    dt.date,
    dt.day_of_week,
    dt.time_of_day,
    dpm.payment_method,
    dpm.payment_risk_level,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    SUM(CASE WHEN ft.is_fraud = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS fraud_rate,
    AVG(ft.amount) AS avg_transaction_amount,
    AVG(CASE WHEN ft.is_fraud = 1 THEN ft.amount ELSE NULL END) AS avg_fraud_amount
FROM {{ ref('fact_transactions') }} ft
JOIN {{ ref('dim_merchant') }} dm ON ft.merchant_id = dm.merchant_id
JOIN {{ ref('dim_location') }} dl ON ft.location_key = dl.location_key
JOIN {{ ref('dim_time') }} dt ON ft.time_key = dt.time_key
JOIN {{ ref('dim_payment_method') }} dpm ON ft.payment_method_key = dpm.payment_method_key
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9