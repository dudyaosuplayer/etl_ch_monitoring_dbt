with customers as (
    select
        customer_id,
        first_name,
        last_name,
        company,
        country
    from {{ ref('stg_customers') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_amount,
        order_date
    from {{ ref('stg_orders') }}
),
customer_order_stats as (
    select
        customer_id,
        count(order_id) as total_orders,
        sum(order_amount) as total_amount,
        avg(order_amount) as avg_order_amount,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from orders
    group by customer_id
)

select
    c.customer_id,
    concat(c.first_name, ' ', c.last_name) as full_name,
    c.company,
    c.country,
    s.total_orders,
    s.total_amount,
    round(s.avg_order_amount, 2) as avg_order_amount,
    s.first_order_date,
    s.last_order_date,
    dateDiff(
        'day',
        s.last_order_date,
        today()
    ) as days_since_last_order,

    case
        when s.total_amount >= 10000 then 'VIP'
        when s.total_amount >= 5000 then 'Regular'
        else 'New'
    end as customer_segment

from customers c

left join customer_order_stats s
    on c.customer_id = s.customer_id