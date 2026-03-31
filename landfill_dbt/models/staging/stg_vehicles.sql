WITH base AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY (SELECT NULL)) AS rn
    FROM {{ source('raw', 'vehicles') }}
)
SELECT
vehicle_id,
vehicle_number,
vehicle_type,
owner_customer_id,
capacity_tons
FROM base
WHERE rn = 1
