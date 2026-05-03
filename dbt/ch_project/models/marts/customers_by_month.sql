{{ config(materialized='table') }}

select
    toStartOfMonth(subscription_date) as month,
    count(*) as customers_count
from {{ ref('stg_customers') }}
group by month
order by month