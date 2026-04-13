from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def latest_record_per_key(df: DataFrame, business_key: str, event_ts_col: str = "event_ts") -> DataFrame:
    window_spec = Window.partitionBy(business_key).orderBy(F.col(event_ts_col).desc())
    ranked = df.withColumn("_rn", F.row_number().over(window_spec))
    return ranked.filter(F.col("_rn") == 1).drop("_rn")


def filter_soft_deletes(df: DataFrame, op_col: str = "op") -> DataFrame:
    return df.filter(F.col(op_col) != F.lit("D"))
