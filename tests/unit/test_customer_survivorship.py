from __future__ import annotations

from pyspark.sql import Row

from src.transforms.bronze_to_silver.customers import build_conformed_customers


def test_build_conformed_customers_generates_surrogate_key(spark):
    source_a = spark.createDataFrame(
        [Row(customer_id="A100", email="amy@example.com", country="CA", status="active",
             source_system="source_a", event_ts="2026-01-01T00:00:00", op="I")]
    )
    source_b = spark.createDataFrame(
        [Row(cust_id="B900", email_address="amy@example.com", region="Canada", is_active=True,
             source_system="source_b", event_ts="2026-01-02T00:00:00", op="U")]
    )

    result = build_conformed_customers(source_a, source_b)
    assert "customer_sk" in result.columns
