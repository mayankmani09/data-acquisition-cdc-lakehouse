from __future__ import annotations

from pathlib import Path

from src.common.logger import get_logger
from src.common.utils import ensure_dir

logger = get_logger(__name__)


def land_raw_file(source_path: str, destination_dir: str) -> str:
    src = Path(source_path)
    dest_dir = ensure_dir(destination_dir)
    dest = dest_dir / src.name
    dest.write_bytes(src.read_bytes())
    logger.info("Landed raw file %s -> %s", src, dest)
    return str(dest)
