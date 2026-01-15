from __future__ import annotations

from fintech_news_scraper.types import Article


def is_breaking(cfg_raw: dict, article: Article) -> bool:
    """Return True if an article meets the configured "breaking" threshold."""

    bn = cfg_raw.get("breaking_news", {})
    if not bool(bn.get("enabled", True)):
        return False

    min_score = float(bn.get("min_score", 0.55))
    if article.is_duplicate:
        return False
    return float(article.score or 0.0) >= min_score


# Backwards-compatible alias (notifications/toasts removed).
should_notify_breaking = is_breaking
