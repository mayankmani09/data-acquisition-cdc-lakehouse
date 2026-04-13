from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("acquisition-cdc-lakehouse-tests")
        .getOrCreate()
    )
    yield session
    session.stop()
