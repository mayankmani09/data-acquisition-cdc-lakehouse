from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_source_a_payments(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("payment_id").alias("source_payment_id"),
        F.col("order_id").alias("source_order_id"),
        F.col("payment_amount").alias("payment_amount"),
        F.col("payment_status").alias("payment_status"),
        F.col("source_system"),
    )


def normalize_source_b_payments(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("pay_id").alias("source_payment_id"),
        F.col("ord_id").alias("source_order_id"),
        F.col("amount_paid").alias("payment_amount"),
        F.col("status").alias("payment_status"),
        F.col("source_system"),
    )


def build_conformed_payments(source_a_df: DataFrame, source_b_df: DataFrame) -> DataFrame:
    return normalize_source_a_payments(source_a_df).unionByName(normalize_source_b_payments(source_b_df))
