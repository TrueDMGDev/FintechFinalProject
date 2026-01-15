from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from typing import Any

import aiohttp

from fintech_news_scraper.config import load_config, load_yaml
from fintech_news_scraper.dedup import dedup_against_recent
from fintech_news_scraper.discover import discover_links_from_html
from fintech_news_scraper.extract import (
    extract_main_text,
    extract_text_from_html_fragment,
    extract_title,
    looks_like_login_or_paywall,
)
from fintech_news_scraper.http import DomainRateLimiter, HttpClient, RetryPolicy
from fintech_news_scraper.nlp import (
    auto_tags,
    breaking_score,
    extract_keywords_tfidf,
    fallback_entities,
    normalize_text,
    try_extract_entities_spacy,
)
from fintech_news_scraper.rss import fetch_rss_entries, rss_entry_to_article
from fintech_news_scraper.storage import articles_to_frame, read_existing, upsert_file
from fintech_news_scraper.types import Article


async def _scrape_article(client: HttpClient, a: Article) -> Article:
    html = await client.get_text(a.url)
    if not html:
        # If the feed provided a summary, use it as best-effort text.
        if a.summary:
            summary_text = normalize_text(extract_text_from_html_fragment(a.summary))
            return replace(a, text=summary_text)
        return a
    title = a.title
    if not title:
        title = extract_title(html) or title
    text = extract_main_text(html)
    text = normalize_text(text)

    # Only discard as login/paywall if extraction is poor.
    if len(text.strip()) < 120 and looks_like_login_or_paywall(html):
        if a.summary:
            summary_text = normalize_text(extract_text_from_html_fragment(a.summary))
            if summary_text:
                return replace(a, title=title, text=summary_text)
        return replace(a, title=title, text=None)

    return replace(a, title=title, text=text)


async def run_pipeline(
    config_path: str,
    sources_path: str,
    max_items: int,
    *,
    source_group: str = "sources",  # "sources" | "breaking_sources" | "all"
    persist: bool = True,
    quiet: bool = False,
    skip_urls: set[str] | None = None,
    recent_texts: list[str] | None = None,
    recent_urls: list[str] | None = None,
) -> list[Article]:
    cfg = load_config(config_path)
    sources_yaml = load_yaml(sources_path)
    sources = sources_yaml.get("sources", [])
    breaking_sources = sources_yaml.get("breaking_sources", [])

    if source_group == "sources":
        active_sources = list(sources)
    elif source_group == "breaking_sources":
        active_sources = list(breaking_sources)
    elif source_group == "all":
        active_sources = list(sources) + list(breaking_sources)
    else:
        raise ValueError("source_group must be one of: sources, breaking_sources, all")

    http_cfg = cfg.raw["http"]
    conc_cfg = cfg.raw["concurrency"]
    rl_cfg = cfg.raw["rate_limit"]
    rt_cfg = cfg.raw["retry"]

    human_cfg = cfg.raw.get("human_mode", {})
    human_delay = None
    if bool(human_cfg.get("enabled", False)):
        human_delay = (
            float(human_cfg.get("min_delay_seconds", 0.2)),
            float(human_cfg.get("max_delay_seconds", 1.0)),
        )

    limiter = DomainRateLimiter(
        max_requests_per_period=int(rl_cfg["max_requests_per_period"]),
        period_seconds=float(rl_cfg["period_seconds"]),
    )

    sem = asyncio.Semaphore(int(conc_cfg["max_in_flight_requests"]))

    retry = RetryPolicy(
        max_attempts=int(rt_cfg["max_attempts"]),
        base_delay_seconds=float(rt_cfg["base_delay_seconds"]),
        max_delay_seconds=float(rt_cfg["max_delay_seconds"]),
        retry_statuses=set(int(x) for x in rt_cfg.get("retry_statuses", [])),
    )

    connector = aiohttp.TCPConnector(limit=int(http_cfg["max_connections"]))

    # 1) RSS ingest (optional)
    rss_articles: list[Article] = []
    rss_enabled = bool((cfg.raw.get("rss", {}) or {}).get("enabled", True))
    if rss_enabled:
        for s in active_sources:
            if not s.get("enabled", False):
                continue
            sid = str(s.get("id"))
            for rss_url in (s.get("rss_urls") or []):
                if not rss_url:
                    continue
                for e in fetch_rss_entries(sid, rss_url, max_items=max_items):
                    rss_articles.append(rss_entry_to_article(e))

    # de-dupe URLs early
    seen: set[str] = set()
    unique_articles: list[Article] = []
    for a in rss_articles:
        if a.url in seen:
            continue
        seen.add(a.url)
        unique_articles.append(a)

    if skip_urls:
        unique_articles = [a for a in unique_articles if a.url not in skip_urls]

    def _interleave(a: list[Article], b: list[Article]) -> list[Article]:
        out: list[Article] = []
        i = 0
        j = 0
        # Simple round-robin to keep variety when we later apply a hard cap.
        while i < len(a) or j < len(b):
            if i < len(a):
                out.append(a[i])
                i += 1
            if j < len(b):
                out.append(b[j])
                j += 1
        return out

    async with aiohttp.ClientSession(connector=connector) as session:
        async def _run_with_client(client: HttpClient) -> list[Article]:
            # 1b) Crawl ingest (seed/listing pages -> discover article URLs)
            crawl_cfg = cfg.raw.get("crawl", {})
            crawl_enabled = bool(crawl_cfg.get("enabled", True))
            max_links_per_seed = int(crawl_cfg.get("max_links_per_seed", 35))
            same_domain_only = bool(crawl_cfg.get("same_domain_only", True))

            discovered_articles: list[Article] = []

            async def crawl_seed(source_id: str, seed_url: str, allow_regex: str | None, deny_regex: str | None) -> None:
                try:
                    html = await client.get_text(seed_url)
                    if not html:
                        return
                    links = discover_links_from_html(
                        seed_url=seed_url,
                        html=html,
                        max_links=max_links_per_seed,
                        allow_regex=allow_regex,
                        deny_regex=deny_regex,
                        same_domain_only=same_domain_only,
                    )
                    for l in links:
                        if not l.url:
                            continue
                        discovered_articles.append(
                            Article(
                                source=source_id,
                                title=l.title or "",
                                url=l.url,
                                published_at=None,
                                summary=None,
                            )
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    return

            crawl_tasks = []
            if crawl_enabled:
                for s in active_sources:
                    if not s.get("enabled", False):
                        continue
                    sid = str(s.get("id"))
                    allow_regex = str(s.get("allow_regex")) if s.get("allow_regex") else None
                    deny_regex = str(s.get("deny_regex")) if s.get("deny_regex") else None
                    for seed in (s.get("crawl_urls") or []):
                        if not seed:
                            continue
                        crawl_tasks.append(crawl_seed(sid, str(seed), allow_regex, deny_regex))

            if crawl_tasks:
                await asyncio.gather(*crawl_tasks, return_exceptions=True)

            # Merge RSS + discovered URLs (interleaved), then de-dupe
            all_candidates = _interleave(unique_articles, discovered_articles)

            seen2: set[str] = set()
            merged: list[Article] = []
            for a in all_candidates:
                if not a.url:
                    continue
                if a.url in seen2:
                    continue
                seen2.add(a.url)
                merged.append(a)

            def _round_robin_by_source(items: list[Article], limit: int) -> list[Article]:
                if limit <= 0:
                    return []
                buckets: dict[str, list[Article]] = {}
                order: list[str] = []
                for it in items:
                    sid = str(it.source or "")
                    if sid not in buckets:
                        buckets[sid] = []
                        order.append(sid)
                    buckets[sid].append(it)

                out: list[Article] = []
                while len(out) < limit:
                    progressed = False
                    for sid in order:
                        b = buckets.get(sid)
                        if not b:
                            continue
                        out.append(b.pop(0))
                        progressed = True
                        if len(out) >= limit:
                            break
                    if not progressed:
                        break
                return out

            # Cap total workload per run to keep it responsive.
            # Use round-robin selection so one fast source doesn't crowd out others.
            max_articles_per_run = int(crawl_cfg.get("max_articles_per_run", 120))
            hard_cap: int | None = None
            if max_articles_per_run > 0:
                hard_cap = max_articles_per_run
            if max_items > 0:
                hard_cap = int(max_items) if hard_cap is None else min(hard_cap, int(max_items))
            if hard_cap is not None and len(merged) > hard_cap:
                merged = _round_robin_by_source(merged, hard_cap)

            if skip_urls:
                merged = [a for a in merged if a.url not in skip_urls]

            if not merged:
                if not quiet:
                    print("No candidates found (RSS/crawl) or all URLs already seen")
                return []

            # 2) Concurrent scraping
            scraped: list[Article] = []

            async def worker(a: Article) -> None:
                scraped.append(await _scrape_article(client, a))

            await asyncio.gather(*(worker(a) for a in merged))

            # Drop low-quality results (e.g., blocked/login pages, empty extraction)
            min_text_chars = int((cfg.raw.get("crawl", {}) or {}).get("min_article_text_chars", 200))
            filtered_scraped: list[Article] = []
            for a in scraped:
                if not a.url:
                    continue
                if not a.text or len(a.text.strip()) < min_text_chars:
                    continue
                filtered_scraped.append(a)
            scraped = filtered_scraped

            if not scraped:
                if not quiet:
                    print("All candidates were filtered (login/paywall/empty text)")
                return []

            # 3) NLP: keywords (batch tf-idf) + entities (spacy) + tags + score
            texts = [a.text or "" for a in scraped]
            kw_lists = extract_keywords_tfidf(texts, top_k=10)

            enriched: list[Article] = []
            for a, kws in zip(scraped, kw_lists, strict=False):
                if a.text:
                    ents = try_extract_entities_spacy(a.text) or fallback_entities(a.text)
                else:
                    ents = []
                tags = auto_tags(kws, ents)
                score = breaking_score(a.text or "", tags, kws)
                enriched.append(replace(a, keywords=kws, entities=ents, tags=tags, score=score))

            # 4) Dedup by similarity against recent stored articles

            use_recent_texts: list[str] = list(recent_texts or [])
            use_recent_urls: list[str] = list(recent_urls or [])

            if persist and not use_recent_texts and not use_recent_urls:
                # GUI usually supplies a recent window; for first-run or other callers,
                # derive a recent window from the existing per-source CSVs.
                try:
                    storage_cfg = cfg.raw.get("storage", {}) or {}
                    out_dir = Path(str(storage_cfg.get("output_dir", "data")))
                    win = int(cfg.raw["dedup"]["compare_window"])

                    recent_texts_acc: list[str] = []
                    recent_urls_acc: list[str] = []
                    if out_dir.exists():
                        for p in sorted(out_dir.glob("news_*.csv")):
                            df = read_existing(p)
                            if df is None or df.empty:
                                continue
                            df2 = df.tail(win)
                            recent_texts_acc.extend([str(x) for x in df2.get("text", []).fillna("").tolist()])
                            recent_urls_acc.extend([str(x) for x in df2.get("url", []).fillna("").tolist()])

                    if recent_texts_acc and recent_urls_acc:
                        use_recent_texts = recent_texts_acc[-win:]
                        use_recent_urls = recent_urls_acc[-win:]
                except Exception:
                    use_recent_texts, use_recent_urls = [], []

            deduped: list[Article] = []
            threshold = float(cfg.raw["dedup"]["similarity_threshold"])
            for a in enriched:
                r = dedup_against_recent(
                    candidate_text=a.text or "",
                    candidate_url=a.url,
                    recent_texts=use_recent_texts,
                    recent_urls=use_recent_urls,
                    threshold=threshold,
                )
                if r.is_duplicate:
                    deduped.append(replace(a, is_duplicate=True, duplicate_of_url=r.duplicate_of_url))
                else:
                    deduped.append(a)

            if persist:
                df_new = articles_to_frame(deduped)
                storage_cfg = cfg.raw.get("storage", {}) or {}
                out_dir = Path(str(storage_cfg.get("output_dir", "data")))

                def _safe_source_id(s: str) -> str:
                    s2 = "".join(ch if (ch.isalnum() or ch in {"_", "-"}) else "_" for ch in (s or "unknown"))
                    return (s2 or "unknown").lower()

                # Persist per-source only (no combined news.csv)
                for src, group in df_new.groupby("source", dropna=False):
                    sid = _safe_source_id(str(src))
                    src_path = out_dir / f"news_{sid}.csv"
                    upsert_file(src_path, group, key="url")

            if not quiet:
                if persist:
                    storage_cfg = cfg.raw.get("storage", {}) or {}
                    out_dir = Path(str(storage_cfg.get("output_dir", "data")))
                    print(f"Fetched: {len(merged)} | Scraped: {len(scraped)} | Stored (per-source): {len(deduped)}")
                    print(f"Output dir: {out_dir}")
                else:
                    print(f"Fetched: {len(merged)} | Scraped: {len(scraped)} | Live (no-save) mode")

            return deduped

        client = HttpClient(
            session=session,
            limiter=limiter,
            retry=retry,
            semaphore=sem,
            user_agent=str(http_cfg["user_agent"]),
            timeout_seconds=int(http_cfg["timeout_seconds"]),
            user_agent_overrides=dict(http_cfg.get("user_agent_overrides") or {}),
            header_overrides=dict(http_cfg.get("header_overrides") or {}),
            human_delay_seconds=human_delay,
        )
        return await _run_with_client(client)
