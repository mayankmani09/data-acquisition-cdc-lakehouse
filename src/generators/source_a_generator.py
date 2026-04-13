from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.utils import ensure_dir


def generate_source_a(output_dir: str = "data/raw_sample/source_a") -> None:
    ensure_dir(output_dir)

    customers = pd.DataFrame(
        [
            {"customer_id": "A100", "email": "amy@example.com", "country": "CA", "status": "active"},
            {"customer_id": "A101", "email": "ben@example.com", "country": "US", "status": "active"},
        ]
    )
    orders = pd.DataFrame(
        [
            {"order_id": "AO1", "customer_id": "A100", "amount": 120.0, "currency": "CAD"},
            {"order_id": "AO2", "customer_id": "A101", "amount": 80.0, "currency": "USD"},
        ]
    )
    payments = pd.DataFrame(
        [
            {"payment_id": "AP1", "order_id": "AO1", "payment_amount": 120.0, "payment_status": "settled"},
            {"payment_id": "AP2", "order_id": "AO2", "payment_amount": 80.0, "payment_status": "settled"},
        ]
    )

    customers.to_csv(Path(output_dir) / "customers_full.csv", index=False)
    orders.to_csv(Path(output_dir) / "orders_full.csv", index=False)
    payments.to_csv(Path(output_dir) / "payments_full.csv", index=False)
