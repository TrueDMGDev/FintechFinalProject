from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup


@dataclass(frozen=True)
class DiscoveredLink:
    url: str
    title: str | None


_DEFAULT_DENY_SUBSTRINGS = (
    "/video/",
    "/live/",
    "/podcast",
    "/subscribe",
    "/signin",
    "/login",
    "/account",
    "#",
    "javascript:",
)


_DEFAULT_TRACKING_PARAMS_PREFIXES = (
    "utm_",
)


_DEFAULT_TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "msclkid",
    "mc_cid",
    "mc_eid",
    "guccounter",
    "guce_referrer",
    "guce_referrer_sig",
    "soc_src",
    "soc_trk",
    "cmpid",
}


_HUB_PATH_SUBSTRINGS = (
    "/topic/",
    "/topics/",
    "/tag/",
    "/tags/",
    "/category/",
    "/categories/",
    "/section/",
    "/sections/",
    "/author/",
    "/authors/",
    "/search",
    "/quote/",
    "/quotes/",
    "/calendar/",
    "/screener/",
)


def _same_domain(seed_url: str, url: str) -> bool:
    try:
        return urlparse(seed_url).netloc.lower() == urlparse(url).netloc.lower()
    except Exception:
        return False


def _normalize_url(seed_url: str, href: str) -> str | None:
    if not href:
        return None
    href = href.strip()
    if any(href.lower().startswith(x) for x in ("mailto:", "tel:")):
        return None
    try:
        return urljoin(seed_url, href)
    except Exception:
        return None


def _strip_fragment_and_tracking_params(url: str) -> str:
    """Remove URL fragments and common tracking params to improve de-duplication."""

    try:
        p = urlparse(url)
        if not p.scheme or not p.netloc:
            return url

        keep_params: list[tuple[str, str]] = []
        for k, v in parse_qsl(p.query, keep_blank_values=False):
            kl = k.lower()
            if any(kl.startswith(prefix) for prefix in _DEFAULT_TRACKING_PARAMS_PREFIXES):
                continue
            if kl in _DEFAULT_TRACKING_PARAMS:
                continue
            keep_params.append((k, v))

        query = urlencode(keep_params, doseq=True)
        p2 = p._replace(query=query, fragment="")
        return urlunparse(p2)
    except Exception:
        return url


_DATE_IN_PATH_RE = re.compile(r"\/\d{4}\/\d{2}\/\d{2}\/|\/\d{4}-\d{2}-\d{2}\/", re.IGNORECASE)


def _looks_like_article_url(url: str) -> bool:
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False

    if _DATE_IN_PATH_RE.search(path):
        return True
    if path.endswith(".html") or path.endswith(".htm"):
        return True
    if "/article/" in path:
        return True
    if "/news/" in path and len(path.split("/")) >= 4:
        return True
    return False


def _is_hub_or_nav_url(url: str) -> bool:
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False
    return any(s in path for s in _HUB_PATH_SUBSTRINGS)


def _score_candidate(seed_url: str, url: str, title: str | None) -> float:
    try:
        p = urlparse(url)
        path = p.path.lower()
    except Exception:
        return -1e9

    score = 0.0

    segs = [s for s in path.split("/") if s]
    score += min(len(segs), 8) * 0.4

    # Penalize section roots (these are usually listing pages, not articles)
    if len(segs) == 1 and segs[0] in {"news", "business", "markets", "world", "finance"}:
        score -= 10.0

    if _looks_like_article_url(url):
        score += 8.0
    if _DATE_IN_PATH_RE.search(path):
        score += 4.0
    if path.endswith(".html") or path.endswith(".htm"):
        score += 2.0

    last = segs[-1] if segs else ""
    if "-" in last:
        score += 1.5

    if _is_hub_or_nav_url(url):
        score -= 8.0

    if path in {"/", ""}:
        score -= 10.0

    try:
        if _strip_fragment_and_tracking_params(url).rstrip("/") == _strip_fragment_and_tracking_params(seed_url).rstrip("/"):
            score -= 10.0
    except Exception:
        pass

    if title:
        t = title.strip()
        if len(t) >= 16:
            score += 0.6
        elif len(t) <= 5:
            score -= 0.6

    if p.query:
        score -= 0.5

    return score


def discover_links_from_html(
    *,
    seed_url: str,
    html: str,
    max_links: int = 50,
    scan_limit: int = 1500,
    allow_regex: str | None = None,
    deny_regex: str | None = None,
    same_domain_only: bool = True,
) -> list[DiscoveredLink]:
    """Extract candidate article links from a listing/home page.

    This is heuristic-based: it finds <a href> links, normalizes them,
    filters obvious non-article URLs, and optionally applies allow/deny regex.
    """

    soup = BeautifulSoup(html or "", "lxml")

    allow_re = re.compile(allow_regex) if allow_regex else None
    deny_re = re.compile(deny_regex) if deny_regex else None

    candidates: list[DiscoveredLink] = []
    seen: set[str] = set()

    scanned = 0
    for a in soup.find_all("a", href=True):
        if scan_limit > 0 and scanned >= scan_limit:
            break
        scanned += 1

        href = str(a.get("href") or "")
        url = _normalize_url(seed_url, href)
        if not url:
            continue

        url = _strip_fragment_and_tracking_params(url)

        url_l = url.lower()
        if any(s in url_l for s in _DEFAULT_DENY_SUBSTRINGS):
            continue

        if same_domain_only and not _same_domain(seed_url, url):
            continue

        if deny_re and deny_re.search(url):
            continue
        if allow_re and not allow_re.search(url):
            continue

        # Drop obvious non-content paths
        path = urlparse(url).path
        if path in {"/", ""}:
            continue

        # Drop section roots like /news/ or /markets/
        segs = [s for s in path.lower().split("/") if s]
        if len(segs) == 1 and segs[0] in {"news", "business", "markets", "world", "finance"}:
            continue

        if url_l in seen:
            continue
        seen.add(url_l)

        title = a.get_text(" ", strip=True) or None
        candidates.append(DiscoveredLink(url=url, title=title))

    scored = [(float(_score_candidate(seed_url, c.url, c.title)), c) for c in candidates]
    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for _s, c in scored[:max_links]]
