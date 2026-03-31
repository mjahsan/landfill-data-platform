SELECT

customer_id,
customer_name,

COUNT(transaction_id) as total_transactions,

SUM(incoming_weight_tons) as total_weight,

SUM(total_charge) as total_revenue,

AVG(total_charge) as avg_transaction_value

FROM {{ ref('silver_transactions') }}

GROUP BY
customer_id,
customer_name
