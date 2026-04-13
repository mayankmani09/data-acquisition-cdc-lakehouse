from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transforms.cdc_merge import latest_record_per_key


def normalize_source_a_customers(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("customer_id").alias("source_customer_id"),
        F.col("email").alias("email"),
        F.col("country").alias("country"),
        F.col("status").alias("status"),
        F.col("source_system"),
        F.col("event_ts"),
        F.col("op"),
    )


def normalize_source_b_customers(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("cust_id").alias("source_customer_id"),
        F.col("email_address").alias("email"),
        F.when(F.col("region") == "Canada", F.lit("CA")).otherwise(F.lit("US")).alias("country"),
        F.when(F.col("is_active"), F.lit("active")).otherwise(F.lit("inactive")).alias("status"),
        F.col("source_system"),
        F.col("event_ts"),
        F.col("op"),
    )


def build_conformed_customers(source_a_df: DataFrame, source_b_df: DataFrame) -> DataFrame:
    unified = normalize_source_a_customers(source_a_df).unionByName(normalize_source_b_customers(source_b_df))
    latest = latest_record_per_key(unified, business_key="email")
    return latest.withColumn("customer_sk", F.sha2(F.col("email"), 256))
