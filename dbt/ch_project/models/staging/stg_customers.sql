select
    idx,
    customer_id,
    lower(trim(first_name)) as first_name,
    lower(trim(last_name)) as last_name,
    company,
    city,
    country,
    phone_1,
    phone_2,
    lower(email) as email,
    toDate(subscription_date) as subscription_date,
    website

from {{ source('raw', 'customers') }}

where 1=1
    and email != ''
    and subscription_date >= '2000-01-01'