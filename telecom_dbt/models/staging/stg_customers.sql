WITH deduplicated AS (

    SELECT *,

           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY created_at DESC
           ) AS rn

    FROM {{ source('telecom_staging', 'stg_customers') }}

)

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

    is_active,

    created_at

FROM deduplicated

WHERE rn = 1