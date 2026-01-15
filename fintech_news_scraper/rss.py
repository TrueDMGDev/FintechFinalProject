from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import feedparser
from dateutil import parser as dateparser

from fintech_news_scraper.types import Article


@dataclass(frozen=True)
class RssEntry:
    source: str
    title: str
    url: str
    published_at: Optional[datetime]
    summary: Optional[str]


def _parse_dt(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = dateparser.parse(value)
        if dt is None:
            return None
        # Ensure tz-aware for consistent comparisons
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def fetch_rss_entries(source_id: str, rss_url: str, max_items: int) -> list[RssEntry]:
    feed = feedparser.parse(rss_url)
    entries: list[RssEntry] = []

    for e in (feed.entries or [])[:max_items]:
        title = getattr(e, "title", None) or ""
        url = getattr(e, "link", None) or ""
        summary = getattr(e, "summary", None)

        published_at = None
        if getattr(e, "published", None):
            published_at = _parse_dt(getattr(e, "published"))
        elif getattr(e, "updated", None):
            published_at = _parse_dt(getattr(e, "updated"))

        if title and url:
            entries.append(RssEntry(source=source_id, title=title, url=url, published_at=published_at, summary=summary))

    return entries


def rss_entry_to_article(e: RssEntry) -> Article:
    return Article(source=e.source, title=e.title, url=e.url, published_at=e.published_at, summary=e.summary)
