select
    current_date as revenue_date,
    sum(order_amount) as total_revenue
from {{ ref('int_order_enriched') }}
