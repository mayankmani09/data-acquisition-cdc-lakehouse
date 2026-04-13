from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.utils import ensure_dir, utc_now


def generate_cdc_batch(batch_id: str, output_dir: str = "data/cdc_sample") -> None:
    batch_dir = ensure_dir(Path(output_dir) / batch_id)

    customer_updates = pd.DataFrame(
        [
            {
                "source_system": "source_a",
                "domain": "customers",
                "op": "U",
                "event_ts": utc_now().isoformat(),
                "customer_id": "A100",
                "email": "amy@example.com",
                "country": "CA",
                "status": "inactive",
            },
            {
                "source_system": "source_b",
                "domain": "customers",
                "op": "I",
                "event_ts": utc_now().isoformat(),
                "cust_id": "B902",
                "email_address": "dina@example.com",
                "region": "Canada",
                "is_active": True,
            },
        ]
    )

    customer_updates.to_csv(Path(batch_dir) / "customers_cdc.csv", index=False)
