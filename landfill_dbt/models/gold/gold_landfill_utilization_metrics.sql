SELECT

landfill_id,
site_name,
region,

SUM(incoming_weight_tons) as total_weight,

COUNT(transaction_id) as total_transactions,

SUM(total_charge) as total_revenue

FROM {{ ref('silver_transactions') }}

GROUP BY
landfill_id,
site_name,
region

