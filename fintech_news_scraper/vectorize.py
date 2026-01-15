from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass

import numpy as np


_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_\-]{1,}")


def _tokenize(text: str) -> list[str]:
    # Keep it simple, fast, and deterministic (English-centric)
    return [t.lower() for t in _WORD_RE.findall(text or "")]


def _ngrams(tokens: list[str], ngram_range: tuple[int, int]) -> list[str]:
    lo, hi = ngram_range
    out: list[str] = []
    for n in range(lo, hi + 1):
        if n == 1:
            out.extend(tokens)
            continue
        for i in range(0, max(0, len(tokens) - n + 1)):
            out.append(" ".join(tokens[i : i + n]))
    return out


@dataclass(frozen=True)
class TfidfModel:
    vocab: dict[str, int]
    idf: np.ndarray  # shape: (V,)


def fit_tfidf(
    texts: list[str],
    *,
    max_features: int = 5000,
    ngram_range: tuple[int, int] = (1, 2),
    min_df: int = 2,
) -> TfidfModel:
    # Build document frequency
    df_counter: Counter[str] = Counter()
    for text in texts:
        tokens = _ngrams(_tokenize(text), ngram_range)
        df_counter.update(set(tokens))

    # Filter by min_df
    items = [(t, c) for t, c in df_counter.items() if c >= min_df]
    # Prefer high df tokens (more stable), then lexicographic for determinism
    items.sort(key=lambda x: (-x[1], x[0]))
    items = items[:max_features]

    vocab = {t: i for i, (t, _) in enumerate(items)}

    n_docs = max(1, len(texts))
    idf = np.zeros((len(vocab),), dtype=np.float32)
    for term, idx in vocab.items():
        df = float(df_counter.get(term, 0))
        # smooth idf (like sklearn-ish)
        idf[idx] = float(math.log((1.0 + n_docs) / (1.0 + df)) + 1.0)

    return TfidfModel(vocab=vocab, idf=idf)


def transform_tfidf(texts: list[str], model: TfidfModel, *, ngram_range: tuple[int, int] = (1, 2)) -> np.ndarray:
    V = len(model.vocab)
    X = np.zeros((len(texts), V), dtype=np.float32)

    for row_idx, text in enumerate(texts):
        toks = _ngrams(_tokenize(text), ngram_range)
        if not toks:
            continue

        tf = Counter(toks)
        for term, count in tf.items():
            col = model.vocab.get(term)
            if col is None:
                continue
            X[row_idx, col] = float(count)

    # TF-IDF
    X *= model.idf

    # L2 normalize rows so dot-product == cosine similarity
    norms = np.linalg.norm(X, axis=1)
    norms[norms == 0] = 1.0
    X = X / norms[:, None]

    return X
