from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class Article:
    source: str
    title: str
    url: str
    published_at: Optional[datetime]
    summary: Optional[str] = None

    # populated after fetch
    text: Optional[str] = None
    authors: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    entities: list[dict[str, Any]] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)

    # scoring / dedup
    score: float = 0.0
    is_duplicate: bool = False
    duplicate_of_url: Optional[str] = None
