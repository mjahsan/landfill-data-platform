WITH base AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY (SELECT NULL)) AS rn
    FROM {{ source('raw', 'transactions') }}
)
SELECT
transaction_id,
landfill_id,
customer_id,
vehicle_id,
transaction_date,
CAST(transaction_time AS TIME) AS transaction_time,
waste_category,
incoming_weight_tons,
charge_per_ton,
total_charge,
payment_status
FROM base
WHERE rn  = 1
