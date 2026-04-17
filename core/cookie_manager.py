"""CookieManager — solve Cloudflare once headful, then inject cookies for headless sessions."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

from core.config import BASE_URLS, JobConfig

log = logging.getLogger("arablocal")

# cf_clearance cookie TTL is ~30 minutes; refresh before expiry
CF_COOKIE_TTL = 25 * 60  # 25 minutes (safety margin)
SOLVE_TIMEOUT = 90  # seconds — hard timeout per headful solve


@dataclass
class CookieEntry:
    cookies: List[dict]
    domain: str
    solved_at: float = 0.0

    @property
    def expired(self) -> bool:
        return (time.monotonic() - self.solved_at) > CF_COOKIE_TTL

    @property
    def age_str(self) -> str:
        age = time.monotonic() - self.solved_at
        return f"{age:.0f}s"


class CookieManager:
    """Singleton-style manager that solves CF once per domain and caches cookies.

    Thread-safe via asyncio.Lock — max 1 headful browser at a time.
    """

    def __init__(self):
        self._store: Dict[str, CookieEntry] = {}  # domain → CookieEntry
        self._solve_lock = asyncio.Lock()
        self._status_callback: Optional[Callable[[str, str], None]] = None

    def set_status_callback(self, cb: Callable[[str, str], None]):
        """Set callback(country_key, status_message) for UI updates."""
        self._status_callback = cb

    def _emit_status(self, country: str, msg: str):
        if self._status_callback:
            self._status_callback(country, msg)

    @staticmethod
    def _domain_for_country(country_key: str) -> str:
        url = BASE_URLS.get(country_key, "")
        return urlparse(url).netloc if url else ""

    def get_cookies(self, country_key: str) -> Optional[List[dict]]:
        domain = self._domain_for_country(country_key)
        entry = self._store.get(domain)
        if entry and not entry.expired:
            return entry.cookies
        return None

    def is_expired(self, country_key: str) -> bool:
        domain = self._domain_for_country(country_key)
        entry = self._store.get(domain)
        if not entry:
            return True
        return entry.expired

    def cookie_status(self, country_key: str) -> str:
        """Return human-readable cookie status for UI."""
        domain = self._domain_for_country(country_key)
        entry = self._store.get(domain)
        if not entry:
            return "none"
        if entry.expired:
            return "expired"
        return "alive"

    async def probe_cf(self, base_url: str, proxies: Optional[List[str]] = None) -> bool:
        """Quick headless fetch to check if CF challenge is present.

        Returns True if CF is detected, False if page loads normally.
        """
        from scrapling.fetchers import AsyncStealthySession, ProxyRotator
        from core.engine import ArabLocalEngine

        url = f"{base_url}/business"
        session_kwargs = dict(
            headless=True,
            timeout=20000,
            disable_resources=True,
            block_ads=True,
        )
        if proxies:
            session_kwargs["proxy_rotator"] = ProxyRotator(proxies)

        session = AsyncStealthySession(**session_kwargs)
        try:
            await session.start()
            page = await session.fetch(url=url)
            if ArabLocalEngine.is_cf_challenge(page):
                return True
            # Also check if we got real content
            hrefs = page.css("a[href*='/business/category/']::attr(href)").getall()
            if not hrefs:
                hrefs = page.css("a[href*='/business/categories/']::attr(href)").getall()
            return len(hrefs) == 0  # No categories = likely blocked
        except Exception as e:
            log.warning(f"[cookie] CF probe failed for {base_url}: {e}")
            return True  # Assume CF if probe fails
        finally:
            try:
                await session.close()
            except Exception:
                pass

    async def solve_for_domain(
        self,
        country_key: str,
        base_url: str,
        proxies: Optional[List[str]] = None,
    ) -> Optional[List[dict]]:
        """Solve CF via headful browser for one domain. Returns cookies or None."""
        from scrapling.fetchers import AsyncStealthySession, ProxyRotator

        domain = self._domain_for_country(country_key)

        # Check cache first
        entry = self._store.get(domain)
        if entry and not entry.expired:
            log.info(f"[cookie] Using cached cookies for {domain} (age: {entry.age_str})")
            return entry.cookies

        async with self._solve_lock:
            # Double-check after acquiring lock (another coroutine may have solved)
            entry = self._store.get(domain)
            if entry and not entry.expired:
                return entry.cookies

            self._emit_status(country_key, "Solving Cloudflare (headful browser)...")
            log.info(f"[cookie] Solving CF for {domain} via headful browser...")

            url = f"{base_url}/business"
            session_kwargs = dict(
                headless=False,
                timeout=60000,
            )
            if proxies:
                session_kwargs["proxy_rotator"] = ProxyRotator(proxies)

            session = AsyncStealthySession(**session_kwargs)
            try:
                await session.start()
                page = await asyncio.wait_for(
                    session.fetch(url=url, solve_cloudflare=True),
                    timeout=SOLVE_TIMEOUT,
                )

                # Extract cookies from the browser context
                cookies = await session.context.cookies()
                cf_cookies = [c for c in cookies if c.get("name") == "cf_clearance"]

                if cf_cookies:
                    self._store[domain] = CookieEntry(
                        cookies=cookies,
                        domain=domain,
                        solved_at=time.monotonic(),
                    )
                    self._emit_status(country_key, "Cloudflare solved")
                    log.info(
                        f"[cookie] CF solved for {domain} — "
                        f"{len(cookies)} cookies captured "
                        f"(cf_clearance found)"
                    )
                    return cookies
                else:
                    # No cf_clearance but page may have loaded fine
                    # Check if we actually got content
                    hrefs = page.css("a[href*='/business/category/']::attr(href)").getall()
                    if not hrefs:
                        hrefs = page.css("a[href*='/business/categories/']::attr(href)").getall()
                    if hrefs:
                        # Page loaded without needing cf_clearance — no CF on this domain
                        self._store[domain] = CookieEntry(
                            cookies=cookies,
                            domain=domain,
                            solved_at=time.monotonic(),
                        )
                        self._emit_status(country_key, "No CF protection detected")
                        log.info(f"[cookie] No CF on {domain} — page loaded directly")
                        return cookies
                    else:
                        self._emit_status(country_key, "CF solve failed — no cookies obtained")
                        log.warning(f"[cookie] CF solve produced no cf_clearance for {domain}")
                        return None

            except asyncio.TimeoutError:
                self._emit_status(country_key, "CF solve timed out (90s)")
                log.error(f"[cookie] CF solve timed out for {domain}")
                return None
            except Exception as e:
                self._emit_status(country_key, f"CF solve error: {str(e)[:50]}")
                log.error(f"[cookie] CF solve failed for {domain}: {e}")
                return None
            finally:
                try:
                    await session.close()
                except Exception:
                    pass

    async def solve_all(
        self,
        jobs: List[JobConfig],
        proxies: Optional[List[str]] = None,
    ) -> Dict[str, Optional[List[dict]]]:
        """Solve CF for all unique domains in the job list. Sequential (lock-protected)."""
        results: Dict[str, Optional[List[dict]]] = {}
        seen_domains: set = set()

        for job in jobs:
            domain = self._domain_for_country(job.country_key)
            if domain in seen_domains:
                # Reuse cookies from same domain
                results[job.country_key] = self.get_cookies(job.country_key)
                continue
            seen_domains.add(domain)

            self._emit_status(job.country_key, "Checking Cloudflare...")
            needs_cf = await self.probe_cf(job.base_url, proxies)

            if needs_cf:
                cookies = await self.solve_for_domain(
                    job.country_key, job.base_url, proxies
                )
                results[job.country_key] = cookies
            else:
                self._emit_status(job.country_key, "No CF protection")
                log.info(f"[cookie] {job.country_key}: no CF detected, skipping solve")
                results[job.country_key] = None

        return results

    async def inject_into(self, session, country_key: str) -> bool:
        """Inject cached cookies into a running session's browser context."""
        cookies = self.get_cookies(country_key)
        if not cookies:
            return False

        try:
            await session.context.add_cookies(cookies)
            log.info(f"[cookie] Injected {len(cookies)} cookies for {country_key}")
            return True
        except Exception as e:
            log.warning(f"[cookie] Failed to inject cookies for {country_key}: {e}")
            return False

    async def refresh(
        self,
        country_key: str,
        base_url: str,
        proxies: Optional[List[str]] = None,
    ) -> Optional[List[dict]]:
        """Force-refresh cookies for a domain (e.g., after mid-scrape CF detection)."""
        domain = self._domain_for_country(country_key)
        # Invalidate current entry
        self._store.pop(domain, None)
        self._emit_status(country_key, "Refreshing Cloudflare cookies...")
        return await self.solve_for_domain(country_key, base_url, proxies)


# Module-level singleton
_cookie_manager: Optional[CookieManager] = None


def get_cookie_manager() -> CookieManager:
    """Get or create the global CookieManager singleton."""
    global _cookie_manager
    if _cookie_manager is None:
        _cookie_manager = CookieManager()
    return _cookie_manager
