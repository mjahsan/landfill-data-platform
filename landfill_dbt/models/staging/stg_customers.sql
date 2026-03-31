WITH base AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY (SELECT NULL)) AS rn
    FROM {{ source('raw', 'customers') }}
)

SELECT
    customer_id,
    TRIM(customer_name) AS customer_name,
    TRIM(UPPER(customer_type)) AS customer_type,
    contact_number,
    email,
    registered_on
FROM base
WHERE rn = 1
