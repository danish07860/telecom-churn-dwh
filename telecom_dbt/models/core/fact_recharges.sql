SELECT

r.recharge_id,
r.customer_id,
d.customer_name,
d.customer_segment,
r.recharge_timestamp,
r.recharge_amount,
r.payment_mode,
r.successful_flag,

    CASE
        WHEN r.recharge_amount >= 500 THEN 'HIGH_VALUE'
        WHEN r.recharge_amount >= 200 THEN 'MEDIUM_VALUE'
        ELSE 'LOW_VALUE'
    END AS recharge_category

FROM {{ ref('stg_recharges') }} r

LEFT JOIN {{ ref('dim_customer') }} d
    ON r.customer_id = d.customer_id