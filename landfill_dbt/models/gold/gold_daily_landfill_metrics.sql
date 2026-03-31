SELECT

transaction_date,

site_name,

region,

COUNT(transaction_id) as total_transactions,

SUM(incoming_weight_tons) as total_weight_tons,

SUM(total_charge) as total_revenue,

AVG(incoming_weight_tons) as avg_weight_per_transaction

FROM {{ ref('silver_transactions') }}

GROUP BY
transaction_date,
site_name,
region
