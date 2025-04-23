{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

SELECT
    customer_id,
    customer_name,
    customer_email,
    customer_age,
    customer_segment,
    loyalty_points,
    CASE
        WHEN customer_age < 25 THEN 'Young Adult'
        WHEN customer_age BETWEEN 25 AND 40 THEN 'Adult'
        WHEN customer_age BETWEEN 41 AND 60 THEN 'Middle Age'
        ELSE 'Senior'
    END AS age_group
FROM {{ ref('stg_processed_fraud_transactions') }}
GROUP BY 1, 2, 3, 4, 5, 6