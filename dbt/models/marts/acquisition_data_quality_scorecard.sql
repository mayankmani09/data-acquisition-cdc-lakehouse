select
    current_timestamp as generated_at,
    count(*) as customer_rows,
    sum(case when email is null then 1 else 0 end) as null_email_rows
from {{ ref('customer_360') }}
