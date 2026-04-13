from __future__ import annotations

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CDC_METADATA_SCHEMA = StructType(
    [
        StructField("source_system", StringType(), False),
        StructField("op", StringType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("ingest_ts", TimestampType(), False),
        StructField("batch_id", StringType(), False),
    ]
)
