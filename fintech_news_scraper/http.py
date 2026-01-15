from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import aiohttp



@dataclass
class RetryPolicy:
    max_attempts: int
    base_delay_seconds: float
    max_delay_seconds: float
    retry_statuses: set[int]


class DomainRateLimiter:
    """Simple per-domain token bucket implemented with asyncio primitives."""

    def __init__(self, max_requests_per_period: int, period_seconds: float) -> None:
        self._max = max_requests_per_period
        self._period = period_seconds
        self._domain_locks: dict[str, asyncio.Lock] = {}
        self._domain_times: dict[str, list[float]] = {}

    async def acquire(self, url: str) -> None:
        domain = urlparse(url).netloc.lower()
        lock = self._domain_locks.setdefault(domain, asyncio.Lock())
        loop = asyncio.get_running_loop()

        while True:
            async with lock:
                now = loop.time()
                times = self._domain_times.setdefault(domain, [])
                cutoff = now - self._period
                while times and times[0] < cutoff:
                    times.pop(0)

                if len(times) < self._max:
                    times.append(now)
                    return

                # wait until the oldest token expires
                wait_for = (times[0] + self._period) - now

            await asyncio.sleep(max(0.0, wait_for))


class HttpClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        limiter: DomainRateLimiter,
        retry: RetryPolicy,
        semaphore: asyncio.Semaphore,
        user_agent: str,
        timeout_seconds: int,
        user_agent_overrides: dict[str, str] | None = None,
        header_overrides: dict[str, dict[str, str | None]] | None = None,
        human_delay_seconds: tuple[float, float] | None = None,
    ) -> None:
        self._session = session
        self._limiter = limiter
        self._retry = retry
        self._sem = semaphore
        self._ua = user_agent
        self._ua_overrides = {k.lower(): str(v) for k, v in (user_agent_overrides or {}).items()}
        self._hdr_overrides = {
            domain.lower(): {str(hk): (None if hv is None else str(hv)) for hk, hv in (hmap or {}).items()}
            for domain, hmap in (header_overrides or {}).items()
        }
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._human_delay = human_delay_seconds

    async def get_text(self, url: str) -> Optional[str]:
        domain = urlparse(url).netloc.lower()
        ua = self._ua_overrides.get(domain)
        if ua is None and domain.startswith("www."):
            ua = self._ua_overrides.get(domain.removeprefix("www."))
        if ua is None:
            ua = self._ua

        headers: dict[str, str] = {
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        overrides = self._hdr_overrides.get(domain)
        if overrides is None and domain.startswith("www."):
            overrides = self._hdr_overrides.get(domain.removeprefix("www."))
        if overrides:
            for k, v in overrides.items():
                if v is None:
                    headers.pop(k, None)
                else:
                    headers[k] = v

        for attempt in range(1, self._retry.max_attempts + 1):
            await self._limiter.acquire(url)
            if self._human_delay is not None:
                lo, hi = self._human_delay
                await asyncio.sleep(random.uniform(float(lo), float(hi)))
            async with self._sem:
                try:
                    async with self._session.get(url, headers=headers, timeout=self._timeout) as r:
                        status = r.status
                        if status in self._retry.retry_statuses:
                            raise aiohttp.ClientResponseError(
                                request_info=r.request_info,
                                history=r.history,
                                status=status,
                                message=f"retryable status {status}",
                                headers=r.headers,
                            )
                        if status >= 400:
                            return None
                        return await r.text(errors="ignore")
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt >= self._retry.max_attempts:
                        return None
                    delay = min(
                        self._retry.max_delay_seconds,
                        self._retry.base_delay_seconds * (2 ** (attempt - 1)),
                    )
                    # jitter to avoid thundering herd
                    delay *= random.uniform(0.7, 1.3)
                    await asyncio.sleep(delay)

        return None
