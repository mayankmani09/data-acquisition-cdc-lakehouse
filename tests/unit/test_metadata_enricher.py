from __future__ import annotations

from pyspark.sql import Row

from src.ingestion.metadata_enricher import add_ingestion_metadata


def test_add_ingestion_metadata_adds_expected_columns(spark):
    df = spark.createDataFrame([Row(id=1)])
    result = add_ingestion_metadata(df, source_system="source_a", batch_id="batch_001")

    assert "source_system" in result.columns
    assert "ingest_ts" in result.columns
    assert "batch_id" in result.columns
