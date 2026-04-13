select
    source_payment_id,
    source_order_id,
    payment_amount,
    payment_status,
    source_system
from main.silver_payments_conformed
