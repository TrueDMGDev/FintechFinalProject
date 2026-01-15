from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from fintech_news_scraper.types import Article


def saved_path(output_dir: Path) -> Path:
    return output_dir / "saved.jsonl"


def load_saved(output_dir: Path) -> pd.DataFrame:
    path = saved_path(output_dir)
    if not path.exists():
        return pd.DataFrame([])

    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue

    df = pd.DataFrame(rows)
    return df


def is_saved(output_dir: Path, url: str) -> bool:
    if not url:
        return False
    df = load_saved(output_dir)
    if df.empty or "url" not in df.columns:
        return False
    return bool((df["url"].astype(str) == str(url)).any())


def save_article(output_dir: Path, article: Article) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = saved_path(output_dir)

    # Prevent duplicates
    if is_saved(output_dir, article.url):
        return

    payload = asdict(article)
    # json-friendly datetime
    if payload.get("published_at") is not None:
        payload["published_at"] = str(payload["published_at"])

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def remove_saved(output_dir: Path, url: str) -> None:
    if not url:
        return

    path = saved_path(output_dir)
    if not path.exists():
        return

    kept: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                if str(obj.get("url", "")) == str(url):
                    continue
                kept.append(line.rstrip("\n"))
            except Exception:
                # keep malformed lines as-is
                kept.append(line.rstrip("\n"))

    with open(path, "w", encoding="utf-8") as f:
        for line in kept:
            if line:
                f.write(line + "\n")
