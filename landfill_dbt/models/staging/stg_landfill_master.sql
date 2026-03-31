WITH base AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY landfill_id ORDER BY (SELECT NULL)) AS rn
    FROM {{ source('raw', 'landfill_master') }}
)
SELECT
landfill_id,
site_name,
location,
region,
operating_since,
licensed_capacity_tons,
operator_name,
compliance_status
FROM base
WHERE rn = 1
