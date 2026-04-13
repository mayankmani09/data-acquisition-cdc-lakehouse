from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

from src.common.utils import ensure_dir


def write_quarantine(df: DataFrame, output_dir: str, dataset_name: str) -> str:
    path = ensure_dir(Path(output_dir) / "quarantine" / dataset_name)
    df.write.mode("overwrite").parquet(str(path))
    return str(path)
