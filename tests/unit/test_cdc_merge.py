from __future__ import annotations

from pyspark.sql import Row

from src.transforms.cdc_merge import latest_record_per_key


def test_latest_record_per_key_returns_one_record_per_business_key(spark):
    df = spark.createDataFrame(
        [
            Row(email="amy@example.com", event_ts="2026-01-01T00:00:00", status="active"),
            Row(email="amy@example.com", event_ts="2026-01-02T00:00:00", status="inactive"),
        ]
    )

    result = latest_record_per_key(df, business_key="email")
    rows = result.collect()

    assert len(rows) == 1
