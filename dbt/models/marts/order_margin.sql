select
    source_order_id,
    order_amount,
    payment_amount,
    order_amount - coalesce(payment_amount, 0) as unpaid_balance
from {{ ref('int_order_enriched') }}
