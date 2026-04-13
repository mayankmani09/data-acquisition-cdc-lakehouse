select
    o.source_order_id,
    o.source_customer_id,
    o.order_amount,
    o.currency,
    p.payment_amount,
    p.payment_status
from {{ ref('stg_orders') }} o
left join {{ ref('stg_payments') }} p
    on o.source_order_id = p.source_order_id
