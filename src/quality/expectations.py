from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_not_null(df: DataFrame, column_name: str) -> int:
    return df.filter(F.col(column_name).isNull()).count()


def assert_unique(df: DataFrame, column_name: str) -> int:
    duplicates = (
        df.groupBy(column_name)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    return duplicates
