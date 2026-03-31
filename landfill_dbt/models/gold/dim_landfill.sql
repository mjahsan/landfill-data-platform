SELECT

landfill_id,
site_name,
region,
operating_since,
licensed_capacity_tons,
operator_name,
compliance_status

FROM {{ ref('stg_landfill_master') }}
