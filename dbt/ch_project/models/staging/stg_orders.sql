select 
    order_id,
    customer_id,
    product as product_name,
    amount as order_amount,
    quantity,
    toDate(order_date) as order_date,
    toYear(order_date) as order_year,
    toMonth(order_date) as order_month,
    toDayOfWeek(order_date) as weekday
from {{ source('raw', 'orders') }}
