{{ config(
    materialized='incremental',
    unique_key='call_id'
) }}

SELECT
    c.call_id,
    c.customer_id,
    d.customer_name,
    d.customer_segment, 
    c.call_timestamp,   
    c.call_duration_minutes,    
    c.network_type, 
    c.call_drop_flag,   
    c.tower_location,

    CASE
        WHEN c.call_duration_minutes >= 10 THEN 'LONG_CALL'
        WHEN c.call_duration_minutes >= 5 THEN 'MEDIUM_CALL'
        ELSE 'SHORT_CALL'
    END AS call_category

FROM {{ ref('stg_calls') }} c

LEFT JOIN {{ ref('dim_customer') }} d
    ON c.customer_id = d.customer_id

{% if is_incremental() %}

WHERE c.call_timestamp >
    (
        SELECT MAX(call_timestamp)
        FROM {{ this }}
    )

{% endif %}