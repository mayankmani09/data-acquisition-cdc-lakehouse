select
    source_order_id,
    source_customer_id,
    order_amount,
    currency,
    source_system
from main.silver_orders_conformed
