-- models/marts/core/fact_transactions.sql
{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

{% if is_incremental() %}
-- Xác định timestamp lớn nhất đã có trong bảng đích (target table)
{% set max_loaded_timestamp_query %}
SELECT max(timestamp) FROM {{ this }}
{% endset %}
{% set max_loaded_timestamp = run_query(max_loaded_timestamp_query).columns[0].values()[0] %}

-- Nếu chưa có dữ liệu (lần chạy đầu), đặt giá trị mặc định (ví dụ: một ngày rất xa trong quá khứ)
{% if max_loaded_timestamp is none or max_loaded_timestamp == '' %}
  {% set max_loaded_timestamp = '1900-01-01 00:00:00' %} -- Hoặc một giá trị mặc định phù hợp khác
{% endif %}

{% endif %}

SELECT
    stg.transaction_id,
    stg.customer_id,
    stg.merchant_id,
    stg.device_id,
    stg.payment_method_key,
    stg.location_key,
    stg.time_key,
    stg.amount,
    stg.currency,
    stg.is_fraud,
    stg.transaction_type,
    stg.transaction_status,
    stg.risk_score,
    stg.fraud_score,
    stg.account_balance,
    stg.loyalty_points, 
    stg.fraud_rules,
    stg.timestamp       
FROM {{ ref('stg_processed_fraud_transactions') }} stg

{% if is_incremental() %}
-- Lọc các bản ghi mới hơn timestamp lớn nhất đã có
WHERE stg.timestamp > '{{ max_loaded_timestamp }}'::timestamp_ntz 