{{
    config(
        materialized='table',
        unique_key='merchant_id'
    )
}}

SELECT
    merchant_id,
    merchant_name,
    merchant_category,
    CASE
        WHEN merchant_category IN ('electronics', 'gaming', 'financial') THEN 'High Risk'
        WHEN merchant_category IN ('travel', 'entertainment') THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS risk_category
FROM {{ ref('stg_processed_fraud_transactions') }}
GROUP BY 1, 2, 3