SELECT

transaction_id,

customer_id,
vehicle_id,
landfill_id,

transaction_date,
transaction_time,

waste_category,

incoming_weight_tons,
charge_per_ton,
total_charge,

payment_status

FROM {{ ref('silver_transactions') }}
