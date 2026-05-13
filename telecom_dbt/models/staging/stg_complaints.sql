SELECT

    complaint_id,

    customer_id,

    LOWER(complaint_type) AS complaint_type,

    CAST(complaint_date AS TIMESTAMP) AS complaint_timestamp,

    LOWER(status) AS complaint_status,

    resolution_time_hours

FROM {{ source('telecom_staging', 'stg_complaints') }}