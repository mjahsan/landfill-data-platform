SELECT

t.transaction_id,

DATE(t.transaction_date) as transaction_date,

t.transaction_time,

t.customer_id,
c.customer_name,
c.customer_type,

t.vehicle_id,
v.vehicle_number,
v.vehicle_type,

t.landfill_id,
l.site_name,
l.region,

t.waste_category,

t.incoming_weight_tons,

t.charge_per_ton,

t.total_charge,

CASE 
    WHEN t.total_charge > 10000 THEN 'HIGH_VALUE'
    WHEN t.total_charge > 5000 THEN 'MEDIUM_VALUE'
    ELSE 'LOW_VALUE'
END as transaction_category,

t.payment_status

FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('stg_customers') }} c
ON t.customer_id = c.customer_id
LEFT JOIN {{ ref('stg_vehicles') }} v
ON t.vehicle_id = v.vehicle_id
LEFT JOIN {{ ref('stg_landfill_master') }} l
ON t.landfill_id = l.landfill_id
