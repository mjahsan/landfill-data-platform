SELECT

customer_id,
customer_name,
customer_type,
contact_number,
email,
registered_on

FROM {{ ref('stg_customers') }}
