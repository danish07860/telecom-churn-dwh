{{ config(

    materialized='incremental',

    unique_key='complaint_id'

) }}

SELECT

    c.complaint_id,

    c.customer_id,

    d.customer_name,

    c.complaint_type,

    c.complaint_timestamp,

    c.complaint_status,

    c.resolution_time_hours,

    c.created_at,

    CASE

        WHEN c.resolution_time_hours > 48 THEN 'HIGH'

        WHEN c.resolution_time_hours > 24 THEN 'MEDIUM'

        ELSE 'LOW'

    END AS complaint_severity

FROM {{ ref('stg_complaints') }} c

LEFT JOIN {{ ref('dim_customer') }} d

    ON c.customer_id = d.customer_id


{% if is_incremental() %}

WHERE c.created_at >

(

    SELECT COALESCE(
        MAX(created_at),
        '1900-01-01'
    )

    FROM {{ this }}

)

{% endif %}