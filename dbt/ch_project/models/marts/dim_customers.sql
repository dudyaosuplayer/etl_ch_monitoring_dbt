{{ config(materialized='table') }}

select
    customer_id,
    first_name,
    last_name,
    company,
    city,
    country,
    email,
    subscription_date
from {{ ref('stg_customers') }}