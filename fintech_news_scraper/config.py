from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Config:
    raw: dict[str, Any]

    @property
    def output_dir(self) -> Path:
        return Path(self.raw["storage"]["output_dir"])

    @property
    def output_file(self) -> Path:
        storage = self.raw.get("storage", {})
        name = storage.get("output_file") or storage.get("parquet_file") or "news.csv"
        return self.output_dir / str(name)


def load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(path: str | Path) -> Config:
    return Config(raw=load_yaml(path))
