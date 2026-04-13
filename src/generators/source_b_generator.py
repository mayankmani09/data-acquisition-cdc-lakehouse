from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.utils import ensure_dir


def generate_source_b(output_dir: str = "data/raw_sample/source_b") -> None:
    ensure_dir(output_dir)

    customers = pd.DataFrame(
        [
            {"cust_id": "B900", "email_address": "amy@example.com", "region": "Canada", "is_active": True},
            {"cust_id": "B901", "email_address": "cara@example.com", "region": "USA", "is_active": True},
        ]
    )
    orders = pd.DataFrame(
        [
            {"ord_id": "BO1", "cust_id": "B900", "gross_amount": 119.99, "currency_code": "CAD"},
            {"ord_id": "BO2", "cust_id": "B901", "gross_amount": 50.00, "currency_code": "USD"},
        ]
    )
    payments = pd.DataFrame(
        [
            {"pay_id": "BP1", "ord_id": "BO1", "amount_paid": 119.99, "status": "captured"},
            {"pay_id": "BP2", "ord_id": "BO2", "amount_paid": 50.00, "status": "captured"},
        ]
    )

    customers.to_csv(Path(output_dir) / "customers_full.csv", index=False)
    orders.to_csv(Path(output_dir) / "orders_full.csv", index=False)
    payments.to_csv(Path(output_dir) / "payments_full.csv", index=False)
