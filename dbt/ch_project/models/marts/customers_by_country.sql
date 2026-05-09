select
    country,
    count(*) as customers_count
from {{ ref('stg_customers') }}
group by country