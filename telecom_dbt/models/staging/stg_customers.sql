SELECT

    customer_id,

    customer_full_name AS customer_name,

    UPPER(gender) AS gender,

    age,

    INITCAP(city) AS city,

    INITCAP(state) AS state,

    LOWER(plan_type) AS plan_type,

    monthly_charges,

    tenure_months,

    is_active

FROM {{ source('telecom_staging', 'stg_customers') }}