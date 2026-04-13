select
    customer_sk,
    email,
    country,
    status
from {{ ref('int_customer_latest') }}
