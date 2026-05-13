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
CASE
        WHEN tenure_months >= 24 THEN 'LOYAL'
        WHEN tenure_months >= 12 THEN 'ACTIVE'
        ELSE 'NEW'
    END AS customer_segment
FROM {{ ref('stg_customers') }}