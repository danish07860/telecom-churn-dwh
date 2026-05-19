{{ config(

    materialized='incremental',

    unique_key='customer_sk'

) }}


WITH source_data AS (

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

        created_at,

        MD5(

    CONCAT(

        customer_name,

        gender,

        city,

        state,

        plan_type,

        CAST(monthly_charges AS STRING),

        CAST(tenure_months AS STRING),

        CAST(is_active AS STRING)

    )

) AS record_hash

    FROM {{ ref('stg_customers') }}

),


existing_current AS (

    {% if is_incremental() %}

    SELECT *

    FROM {{ this }}

    WHERE is_current = TRUE

    {% else %}

    SELECT
        NULL AS customer_id,
        NULL AS record_hash,
        NULL AS is_current

    WHERE FALSE

    {% endif %}

),


changed_records AS (

    SELECT

        s.*

    FROM source_data s

    LEFT JOIN existing_current e

        ON s.customer_id = e.customer_id

    WHERE

        e.customer_id IS NULL

        OR s.record_hash != e.record_hash

)


SELECT

    ROW_NUMBER() OVER (

        ORDER BY customer_id

    ) AS customer_sk,

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

    CURRENT_TIMESTAMP() AS effective_from,

    NULL AS effective_to,

    TRUE AS is_current,

    record_hash,

    created_at

FROM changed_records