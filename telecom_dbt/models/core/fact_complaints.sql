SELECT

c.complaint_id,
c.customer_id,
d.customer_name,
d.customer_segment,
c.complaint_type,
c.complaint_timestamp,
c.complaint_status,
c.resolution_time_hours,

    CASE
        WHEN c.resolution_time_hours > 48 THEN 'HIGH'
        WHEN c.resolution_time_hours > 24 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS complaint_severity

FROM {{ ref('stg_complaints') }} c

LEFT JOIN {{ ref('dim_customer') }} d
    ON c.customer_id = d.customer_id