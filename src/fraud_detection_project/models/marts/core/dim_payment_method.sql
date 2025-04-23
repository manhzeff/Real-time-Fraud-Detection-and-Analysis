-- models/marts/core/dim_payment_method.sql
{{
    config(
        materialized='table',
        unique_key='payment_method_key'
    )
}}

SELECT
    MD5(payment_method) AS payment_method_key,
    payment_method,
    -- Phân loại phương thức thanh toán
    CASE
        WHEN payment_method = 'crypto' THEN 'High Risk'
        WHEN payment_method IN ('digital_wallet', 'bank_transfer') THEN 'Medium Risk'
        ELSE 'Standard Risk'
    END AS payment_risk_level
FROM {{ ref('stg_processed_fraud_transactions') }}
GROUP BY 1, 2