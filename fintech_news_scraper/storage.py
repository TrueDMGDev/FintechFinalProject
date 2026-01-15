from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import pandas as pd

from fintech_news_scraper.types import Article


def articles_to_frame(articles: list[Article]) -> pd.DataFrame:
    rows = []
    for a in articles:
        d = asdict(a)
        # Normalize datetime for parquet
        if d.get("published_at") is not None:
            d["published_at"] = pd.to_datetime(d["published_at"], utc=True, errors="coerce")
        rows.append(d)
    return pd.DataFrame(rows)


def read_existing(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None

    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix in {".csv", ".txt"}:
        return pd.read_csv(path)
    # fallback try parquet, then csv
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.read_csv(path)


def write_frame(path: Path, df: pd.DataFrame) -> None:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        try:
            df.to_parquet(path, index=False)
            return
        except ImportError:
            # fall back to CSV next to parquet
            csv_path = path.with_suffix(".csv")
            df.to_csv(csv_path, index=False, encoding="utf-8")
            return

    if suffix in {".csv", ".txt"}:
        df.to_csv(path, index=False, encoding="utf-8")
        return

    # default to csv
    df.to_csv(path, index=False, encoding="utf-8")


def upsert_file(path: Path, new_df: pd.DataFrame, key: str = "url") -> pd.DataFrame:
    path.parent.mkdir(parents=True, exist_ok=True)

    old_df = read_existing(path)
    if old_df is not None:
        combined = pd.concat([old_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=[key], keep="last")
    else:
        combined = new_df.drop_duplicates(subset=[key], keep="last")

    write_frame(path, combined)
    return combined
