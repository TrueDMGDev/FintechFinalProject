from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

import numpy as np

from fintech_news_scraper.vectorize import fit_tfidf, transform_tfidf


_FINANCE_KEYWORDS = {
    "inflation",
    "interest rate",
    "rates",
    "fed",
    "ecb",
    "boe",
    "earnings",
    "revenue",
    "profit",
    "loss",
    "guidance",
    "ipo",
    "bond",
    "yield",
    "stocks",
    "equities",
    "market",
    "oil",
    "gold",
    "bitcoin",
    "crypto",
    "forex",
    "usd",
    "eur",
    "gdp",
    "recession",
    "merger",
    "acquisition",
    "m\u0026a",
}


@dataclass(frozen=True)
class NlpResult:
    keywords: list[str]
    entities: list[dict]
    tags: list[str]
    score: float


def normalize_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_keywords_tfidf(texts: list[str], top_k: int = 10) -> list[list[str]]:
    if not texts:
        return []

    model = fit_tfidf(texts, max_features=5000, ngram_range=(1, 2), min_df=2)
    X = transform_tfidf(texts, model, ngram_range=(1, 2))
    if not getattr(model, "vocab", None):
        return [[] for _ in texts]

    inv_pairs = sorted(model.vocab.items(), key=lambda kv: kv[1])
    if not inv_pairs:
        return [[] for _ in texts]

    inv_vocab = np.array(inv_pairs, dtype=object)[:, 0]

    out: list[list[str]] = []
    for i in range(X.shape[0]):
        row = X[i]
        if float(np.max(row)) == 0.0:
            out.append([])
            continue
        idx = np.argsort(row)[::-1][:top_k]
        out.append([str(inv_vocab[j]) for j in idx if row[j] > 0])
    return out


def try_extract_entities_spacy(text: str) -> list[dict]:
    """Entity recognition with spaCy if model is available.

    If spaCy or its model isn't installed, returns an empty list.
    """

    try:
        import spacy

        try:
            nlp = spacy.load("en_core_web_sm")
        except Exception:
            return []

        doc = nlp(text)
        ents = []
        for ent in doc.ents:
            ents.append({"text": ent.text, "label": ent.label_})
        return ents
    except Exception:
        return []


_MONEY_RE = re.compile(r"(?:\$|€|£)\s?\d+(?:[\.,]\d+)?(?:\s?(?:bn|billion|m|million|k|thousand))?", re.IGNORECASE)
_TICKER_RE = re.compile(r"\b[A-Z]{1,5}\b")


def fallback_entities(text: str) -> list[dict]:
    ents: list[dict] = []
    for m in _MONEY_RE.finditer(text or ""):
        ents.append({"text": m.group(0), "label": "MONEY"})
    # Very rough: all-caps tokens often correspond to tickers/ORG acronyms
    for m in _TICKER_RE.finditer(text or ""):
        tok = m.group(0)
        if tok in {"A", "I"}:
            continue
        ents.append({"text": tok, "label": "ORG"})
    return ents


def auto_tags(keywords: Iterable[str], entities: Iterable[dict]) -> list[str]:
    tags: set[str] = set()

    kw_norm = {k.lower() for k in keywords if k}

    # Finance heuristic tags
    if any("earn" in k for k in kw_norm) or "guidance" in kw_norm:
        tags.add("earnings")
    if "inflation" in kw_norm:
        tags.add("macro")
    if "interest rate" in kw_norm or "rates" in kw_norm:
        tags.add("rates")
    if "oil" in kw_norm or "gold" in kw_norm:
        tags.add("commodities")
    if "bitcoin" in kw_norm or "crypto" in kw_norm:
        tags.add("crypto")
    if "forex" in kw_norm or "usd" in kw_norm or "eur" in kw_norm:
        tags.add("fx")

    # Entities-based tags
    for e in entities:
        label = str(e.get("label", ""))
        if label in {"ORG"}:
            tags.add("companies")
        if label in {"GPE"}:
            tags.add("geography")
        if label in {"MONEY"}:
            tags.add("money")

    return sorted(tags)


def breaking_score(text: str, tags: list[str], keywords: list[str]) -> float:
    """A small heuristic score in [0,1]."""

    if not text:
        return 0.0

    text_l = text.lower()
    score = 0.0

    # urgency cues
    if any(w in text_l for w in ["breaking", "just in", "urgent", "developing"]):
        score += 0.35

    # finance keyword density (very rough)
    hit = 0
    for k in _FINANCE_KEYWORDS:
        if k in text_l:
            hit += 1
    score += min(0.35, hit * 0.05)

    # strong tags
    if any(t in tags for t in ["rates", "macro", "earnings"]):
        score += 0.20
    if any(t in tags for t in ["crypto", "commodities"]):
        score += 0.10

    # keywords presence
    score += min(0.10, len(keywords) * 0.01)

    return float(max(0.0, min(1.0, score)))
