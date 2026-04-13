from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ingestion_metadata(df: DataFrame, source_system: str, batch_id: str) -> DataFrame:
    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
    )
