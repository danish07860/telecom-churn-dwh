{{ config(

    materialized='incremental',

    unique_key='call_id'

) }}


SELECT

    call_id,

    customer_id,

    call_date,

    call_duration_minutes,

    network_type,

    call_drop_flag,

    tower_location,

    created_at

FROM {{ source('staging', 'stg_calls') }}


{% if is_incremental() %}

WHERE created_at >

(

    SELECT MAX(created_at)

    FROM {{ this }}

)

{% endif %}