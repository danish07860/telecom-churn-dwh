{{ config(

    materialized='incremental',

    unique_key='complaint_id'

) }}


SELECT

    c.complaint_id,

    c.customer_id,

    d.customer_full_name,

    d.customer_segment,

    c.complaint_type,

    c.complaint_date,

    c.status,

    c.resolution_time_hours,

    c.created_at,

    CASE

        WHEN c.resolution_time_hours > 48 THEN 'HIGH'

        WHEN c.resolution_time_hours > 24 THEN 'MEDIUM'

        ELSE 'LOW'

    END AS complaint_severity

FROM {{ source('staging', 'stg_complaints') }} c

LEFT JOIN {{ ref('dim_customer') }} d

    ON c.customer_id = d.customer_id


{% if is_incremental() %}

WHERE c.created_at >

(

    SELECT MAX(created_at)

    FROM {{ this }}

)

{% endif %}