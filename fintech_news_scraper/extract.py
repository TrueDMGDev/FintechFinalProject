from __future__ import annotations

import re
from bs4 import BeautifulSoup


_LOGIN_PAYWALL_PATTERNS = (
    r"\bsign\s*in\b",
    r"\blog\s*in\b",
    r"\bsubscribe\b",
    r"\bsubscription\b",
    r"\bcreate\s+an?\s+account\b",
    r"\bregister\b",
    r"\bstart\s+your\s+free\s+trial\b",
    r"\balready\s+a\s+subscriber\b",
    r"\bto\s+continue\b.*\b(sign\s*in|log\s*in|subscribe)\b",
    r"\byou\s+have\s+reached\s+your\s+limit\b",
    r"\baccess\s+denied\b",
)


def extract_title(html: str) -> str | None:
    """Best-effort title extraction from an article page."""

    soup = BeautifulSoup(html or "", "lxml")

    for sel in (
        'meta[property="og:title"]',
        'meta[name="twitter:title"]',
        'meta[name="title"]',
    ):
        tag = soup.select_one(sel)
        if tag and tag.get("content"):
            t = str(tag.get("content") or "").strip()
            if t:
                return t

    h1 = soup.find("h1")
    if h1:
        t = h1.get_text(" ", strip=True)
        if t:
            return t

    if soup.title and soup.title.get_text(strip=True):
        t = soup.title.get_text(" ", strip=True)
        # Many sites append a suffix like " - SiteName"; keep it simple and just return.
        if t:
            return t

    return None


def extract_text_from_html_fragment(html_fragment: str) -> str:
    """Convert an HTML snippet (e.g., RSS summary) to plain text."""

    soup = BeautifulSoup(html_fragment or "", "lxml")
    return soup.get_text(" ", strip=True)


def looks_like_login_or_paywall(html: str) -> bool:
    """Heuristic detection of pages that require login/subscription.

    We keep this intentionally conservative; it is used to drop pages that would
    otherwise yield empty/low-quality text.
    """

    soup = BeautifulSoup(html or "", "lxml")

    # Remove common noisy blocks so we don't trip on "Sign in" links in headers/footers.
    for tag in soup.select("script, style, noscript, nav, footer, header, aside"):
        tag.decompose()

    root = soup.find("article") or soup.body or soup
    visible_text = root.get_text(" ", strip=True)

    text_l = (visible_text or "").lower()
    if not text_l:
        return True

    # Extremely small visible text often indicates a JS shell / blocked page.
    if len(text_l) < 120:
        return True

    # Common login/paywall prompts
    for pat in _LOGIN_PAYWALL_PATTERNS:
        if re.search(pat, text_l, flags=re.IGNORECASE):
            return True

    # Cookie/JS gates
    if "enable javascript" in text_l or "enable cookies" in text_l:
        return True

    # Explicit password field present is a strong indicator
    if soup.select_one('input[type="password"]') is not None:
        return True

    return False


def extract_main_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # Remove common noisy blocks
    for tag in soup.select("script, style, noscript, nav, footer, header, aside"):
        tag.decompose()

    # Prefer <article>, otherwise fallback to body
    root = soup.find("article") or soup.body or soup

    # Join paragraph-like content
    parts: list[str] = []
    for p in root.find_all(["p", "h1", "h2", "h3", "li"], recursive=True):
        text = p.get_text(" ", strip=True)
        if text:
            parts.append(text)

    text = "\n".join(parts)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()
