{{ config(materialized='table') }}

WITH complaint_summary AS (

    SELECT
        customer_id,
        COUNT(*) AS total_complaints

    FROM {{ ref('fact_complaints') }}

    GROUP BY customer_id

),

recharge_summary AS (

    SELECT
        customer_id,
        AVG(recharge_amount) AS avg_recharge_amount,
        COUNT(*) AS total_recharges

    FROM {{ ref('fact_recharges') }}

    GROUP BY customer_id

),

call_summary AS (

    SELECT
        customer_id,
        AVG(call_duration_minutes) AS avg_call_duration,
        COUNT(*) AS total_calls

    FROM {{ ref('fact_calls') }}

    GROUP BY customer_id

)

SELECT

    d.customer_id,

    d.customer_full_name,

    d.customer_segment,

    d.plan_type,

    d.monthly_charges,

    d.tenure_months,

    COALESCE(c.total_complaints, 0) AS total_complaints,

    COALESCE(r.avg_recharge_amount, 0) AS avg_recharge_amount,

    COALESCE(r.total_recharges, 0) AS total_recharges,

    COALESCE(cs.avg_call_duration, 0) AS avg_call_duration,

    COALESCE(cs.total_calls, 0) AS total_calls,

    CASE

        WHEN
            COALESCE(cs.total_calls, 0) >= 20
            AND COALESCE(r.total_recharges, 0) >= 5
        THEN 'HIGH'

        WHEN
            COALESCE(cs.total_calls, 0) >= 10
        THEN 'MEDIUM'

        ELSE 'LOW'

    END AS engagement_score,

    CASE

        WHEN
            COALESCE(c.total_complaints, 0) >= 3
            AND d.tenure_months < 12
            AND COALESCE(r.total_recharges, 0) <= 2
        THEN 'HIGH_RISK'

        WHEN
            COALESCE(c.total_complaints, 0) >= 1
        THEN 'MEDIUM_RISK'

        ELSE 'LOW_RISK'

    END AS churn_risk

FROM {{ ref('dim_customer') }} d

LEFT JOIN complaint_summary c
    ON d.customer_id = c.customer_id

LEFT JOIN recharge_summary r
    ON d.customer_id = r.customer_id

LEFT JOIN call_summary cs
    ON d.customer_id = cs.customer_id