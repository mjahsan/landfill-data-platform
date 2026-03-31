SELECT

vehicle_id,
vehicle_number,
vehicle_type,
owner_customer_id,
capacity_tons

FROM {{ ref('stg_vehicles') }}
