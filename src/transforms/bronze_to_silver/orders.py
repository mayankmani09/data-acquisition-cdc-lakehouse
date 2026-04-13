from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_source_a_orders(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("order_id").alias("source_order_id"),
        F.col("customer_id").alias("source_customer_id"),
        F.col("amount").alias("order_amount"),
        F.col("currency").alias("currency"),
        F.col("source_system"),
    )


def normalize_source_b_orders(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("ord_id").alias("source_order_id"),
        F.col("cust_id").alias("source_customer_id"),
        F.col("gross_amount").alias("order_amount"),
        F.col("currency_code").alias("currency"),
        F.col("source_system"),
    )


def build_conformed_orders(source_a_df: DataFrame, source_b_df: DataFrame) -> DataFrame:
    return normalize_source_a_orders(source_a_df).unionByName(normalize_source_b_orders(source_b_df))
