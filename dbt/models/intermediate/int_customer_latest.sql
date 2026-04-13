select
    customer_sk,
    email,
    country,
    status,
    source_system
from {{ ref('stg_customers') }}
qualify row_number() over (
    partition by email
    order by source_system
) = 1
