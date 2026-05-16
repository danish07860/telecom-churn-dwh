{{ config(

    materialized='incremental',

    unique_key='recharge_id'

) }}


SELECT

    recharge_id,

    customer_id,

    recharge_date,

    recharge_amount,

    payment_mode,

    successful_flag,

    created_at

FROM {{ source('staging', 'stg_recharges') }}


{% if is_incremental() %}

WHERE created_at >

(

    SELECT MAX(created_at)

    FROM {{ this }}

)

{% endif %}