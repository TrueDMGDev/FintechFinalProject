from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from fintech_news_scraper.vectorize import fit_tfidf, transform_tfidf


@dataclass(frozen=True)
class DedupResult:
    is_duplicate: bool
    duplicate_of_url: str | None
    best_similarity: float


def dedup_against_recent(
    candidate_text: str,
    candidate_url: str,
    recent_texts: list[str],
    recent_urls: list[str],
    threshold: float,
) -> DedupResult:
    if not candidate_text or not recent_texts:
        return DedupResult(is_duplicate=False, duplicate_of_url=None, best_similarity=0.0)

    texts = [candidate_text] + recent_texts

    model = fit_tfidf(texts, max_features=8000, ngram_range=(1, 2), min_df=2)
    X = transform_tfidf(texts, model, ngram_range=(1, 2))

    # Similarity of candidate (row 0) to all recent rows (cosine via dot)
    sims = X[1:] @ X[0]
    if sims.size == 0:
        return DedupResult(is_duplicate=False, duplicate_of_url=None, best_similarity=0.0)

    best_idx = int(np.argmax(sims))
    best_sim = float(sims[best_idx])

    if best_sim >= threshold:
        return DedupResult(is_duplicate=True, duplicate_of_url=recent_urls[best_idx], best_similarity=best_sim)

    return DedupResult(is_duplicate=False, duplicate_of_url=None, best_similarity=best_sim)
