WITH deduplicated AS (

    SELECT *,

           ROW_NUMBER() OVER (
               PARTITION BY complaint_id
               ORDER BY created_at DESC
           ) AS rn

    FROM {{ source('telecom_staging', 'stg_complaints') }}

)

SELECT

    complaint_id,

    customer_id,

    LOWER(complaint_type) AS complaint_type,

    CAST(complaint_date AS TIMESTAMP) AS complaint_timestamp,

    LOWER(status) AS complaint_status,

    resolution_time_hours,

    created_at

FROM deduplicated

WHERE rn = 1