SELECT

    recharge_id,

    customer_id,

    CAST(recharge_date AS TIMESTAMP) AS recharge_timestamp,

    recharge_amount,

    LOWER(payment_mode) AS payment_mode,

    successful_flag

FROM {{ source('telecom_staging', 'stg_recharges') }}