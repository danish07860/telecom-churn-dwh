SELECT

    call_id,

    customer_id,

    CAST(call_date AS TIMESTAMP) AS call_timestamp,

    ROUND(call_duration_minutes, 2) AS call_duration_minutes,

    LOWER(network_type) AS network_type,

    call_drop_flag,

    INITCAP(tower_location) AS tower_location

FROM {{ source('telecom_staging', 'stg_calls') }}