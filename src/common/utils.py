from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import uuid


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def generate_run_id() -> str:
    return str(uuid.uuid4())


def ensure_dir(path: str | Path) -> Path:
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj
