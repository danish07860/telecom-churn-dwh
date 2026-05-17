{{ config(

    materialized='incremental',

    unique_key='customer_id'

) }}

SELECT

    customer_id,

    customer_name,

    gender,

    age,

    city,

    state,

    plan_type,

    monthly_charges,

    tenure_months,

    is_active,

    created_at

FROM {{ ref('stg_customers') }}


{% if is_incremental() %}

WHERE created_at >

(

    SELECT COALESCE(
        MAX(created_at),
        '1900-01-01'
    )

    FROM {{ this }}

)

{% endif %}