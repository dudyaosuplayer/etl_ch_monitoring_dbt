select
    order_id,
    order_amount,
    order_date,
    c.first_name || c.last_name as customer_name,
    country,
    company
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id