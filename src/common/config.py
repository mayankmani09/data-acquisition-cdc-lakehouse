from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_config() -> dict[str, Any]:
    base = load_yaml("configs/base.yml")
    env = get_env("APP_ENV", "local")
    env_file = Path(f"configs/{env}.yml")
    if env_file.exists():
        env_config = load_yaml(env_file)
        base.update(env_config)
    return base
