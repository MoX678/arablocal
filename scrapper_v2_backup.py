#!/usr/bin/env python3
"""
ArabLocal Universal Scraper v2
====================== ==========
Scrapes business listings from arablocal.com with:
  - SQLite staging (zero data loss on crash)
  - Incremental raw CSV output (appended per row)
  - Dynamic category discovery
  - Proxy rotation support
  - Priority-sorted CSV export
  - Full CLI control

Usage:
    python scraper_v2.py                              # All categories, full run
   python scraper_v2.py --category health-care       # Single category
    python scraper_v2.py --limit 10                   # Limit business pages
    python scraper_v2.py --proxies proxies.txt        # Use proxy file
    python scraper_v2.py --concurrency 3              # Override concurrency
    python scraper_v2.py --export-only                # Export CSVs from DB
    python scraper_v2.py --dry-run                    # Discover categories only
    python scraper_v2.py --country uae                # Target country (default: uae)
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
import urllib.request

# Force UTF-8 output on Windows to avoid cp1252 encoding errors with Rich
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from scrapling.fetchers import AsyncStealthySession, ProxyRotator
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    Progress, SpinnerColumn, BarColumn, TextColumn,
    TimeElapsedColumn, MofNCompleteColumn,
)
from rich.table import Table
from rich.panel import Panel

# ─── Logging & Console ──────────────────────────────────────────────────────
log = logging.getLogger("arablocal_v2")
log.setLevel(logging.DEBUG)
_fh = logging.FileHandler("scraper_v2.log", encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(_fh)

console = Console()

# ─── Constants ───────────────────────────────────────────────────────────────
BASE_URLS = {
    "uae": "https://uae.arablocal.com",
    "kw": "https://kuwaitlocal.com",
    "sa": "https://arablocal.com",
    "bh": "https://bahrain.arablocal.com",
    "qa": "https://qatar.arablocal.com",
    "om": "https://oman.arablocal.com",
}

# Country codes → phone prefix + display name
COUNTRY_INFO = {
    "uae": {"code": "+971", "name": "UAE"},
    "kw":  {"code": "+965", "name": "Kuwait"},
    "sa":  {"code": "+966", "name": "Saudi Arabia"},
    "bh":  {"code": "+973", "name": "Bahrain"},
    "qa":  {"code": "+974", "name": "Qatar"},
    "om":  {"code": "+968", "name": "Oman"},
}

DB_FILE = "arablocal_staging.db"
RAW_CSV = "arablocal_raw.csv"

SOCIAL_DOMAINS = {
    "facebook.com": "Facebook",
    "fb.com": "Facebook",
    "twitter.com": "Twitter",
    "x.com": "Twitter",
    "instagram.com": "Instagram",
    "linkedin.com": "LinkedIn",
    "snapchat.com": "Snapchat",
    "tiktok.com": "TikTok",
    "youtube.com": "YouTube",
    "pinterest.com": "Pinterest",
    "t.me": "Telegram",
}

# ArabLocal's own social accounts — must be excluded from business profiles
SITE_SOCIAL_BLACKLIST = {
    # UAE
    "arablocaluae971", "arablocal2020", "uaelocalsearch",
    "UC8BTQhTUZMoR0IgxZ6zsmNA",
    # Kuwait
    "kuwaitlocal965", "kuwaitlocal", "KuwaitLocal",
    # Oman
    "arablocaloman", "UCg5UWsTvOlwEvEdbEqZhdmQ",
    # Generic / cross-site
    "arablocal", "company/9552438",
    # Site-owned Telegram
    "+eabbIq4Qtq5iOTU0",
}

IGNORE_DOMAINS = {
    "arablocal.com", "kuwaitlocal.com", "kuwaitadvert.com",
    "google.com", "google.ae",
    "gstatic.com", "cloudflare.com", "googleapis.com", "goo.gl",
    "bit.ly", "galli2delhi.com",
}

# Merge social domains into ignore set for website extraction
IGNORE_FOR_WEBSITE = IGNORE_DOMAINS | set(SOCIAL_DOMAINS.keys())

# ─── Raw CSV columns (superset — dynamic columns added at export) ────────
BASE_FIELDS = [
    "Name", "Phone_1", "Phone_2", "Phone_3", "WhatsApp", "Email",
    "Location", "Area", "Governorate",
    "Country", "About", "Website", "Rating", "Views", "Fax",
]

_SENTINEL = object()  # Signals consumer tasks to shut down


class AdaptiveDelay:
    """Dynamic delay that backs off on failures and speeds up on successes."""

    def __init__(self, base: float = 0.6, minimum: float = 0.2, maximum: float = 10.0):
        self.current = base
        self.minimum = minimum
        self.maximum = maximum
        self._lock = asyncio.Lock()

    async def wait(self):
        jitter = random.uniform(0, self.current * 0.5)
        await asyncio.sleep(self.current + jitter)

    async def on_success(self):
        async with self._lock:
            self.current = max(self.minimum, self.current * 0.85)

    async def on_failure(self):
        async with self._lock:
            self.current = min(self.maximum, self.current * 1.5)


class ArabLocalEngine:
    """Core scraper engine with SQLite staging and stealth browser."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.base_url = BASE_URLS.get(args.country, BASE_URLS["uae"])
        self.country_key = args.country
        self.is_kuwait = args.country == "kw"
        self.phone_prefix = COUNTRY_INFO.get(args.country, COUNTRY_INFO["uae"])["code"]
        self.country_name = COUNTRY_INFO.get(args.country, COUNTRY_INFO["uae"])["name"]
        self.db_path = DB_FILE

        # Dynamic hardware optimization
        cpu_cores = os.cpu_count() or 4
        self.concurrency = args.concurrency or max(1, min(cpu_cores, 8))
        self.semaphore = asyncio.Semaphore(self.concurrency)
        self.db_lock = asyncio.Lock()

        # Stealth browser session (pooled — one browser, many pages)
        self._session: Optional[AsyncStealthySession] = None

        # Proxy pool
        self.proxy_pool: List[str] = []
        if args.proxies:
            self.proxy_pool = self._load_proxies(args.proxies)

        # Counters & stats
        self.scraped_count = 0
        self.skip_count = 0
        self.error_count = 0
        self.limit = args.limit or 0
        self.start_time: float = 0.0
        self.delay = AdaptiveDelay()

        # Locks & flags
        self.csv_lock = asyncio.Lock()
        self.shutdown_event = asyncio.Event()

        # CSV batch buffer (flush every 25 rows)
        self._csv_buffer: List[dict] = []
        self._csv_fieldnames: Optional[List[str]] = None
        self._csv_flush_size = 25

        # Rich progress (set during pipeline)
        self.progress: Optional[Progress] = None
        self.cat_task_id = None
        self.biz_task_id = None

        self._init_db()
        log.info(f"Engine initialized: base={self.base_url}, concurrency={self.concurrency}, "
                 f"proxies={len(self.proxy_pool)}, limit={self.limit or 'unlimited'}")

    # ─── Database ────────────────────────────────────────────────────────

    def _init_db(self):
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS businesses (
                    url TEXT PRIMARY KEY,
                    category TEXT NOT NULL,
                    data JSON NOT NULL,
                    scraped_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS categories (
                    slug TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL,
                    discovered_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    category_slug TEXT PRIMARY KEY,
                    last_page INTEGER NOT NULL DEFAULT 1,
                    urls_found INTEGER NOT NULL DEFAULT 0,
                    completed INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_biz_category ON businesses(category)
            """)
            conn.commit()

    async def _db_insert_business(self, url: str, category: str, data: dict):
        async with self.db_lock:
            with sqlite3.connect(self.db_path, timeout=15) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO businesses (url, category, data, scraped_at) VALUES (?, ?, ?, ?)",
                    (url, category, json.dumps(data, ensure_ascii=False),
                     datetime.now(timezone.utc).isoformat()),
                )

    async def _db_insert_category(self, slug: str, name: str, url: str):
        async with self.db_lock:
            with sqlite3.connect(self.db_path, timeout=15) as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO categories (slug, name, url, discovered_at) VALUES (?, ?, ?, ?)",
                    (slug, name, url, datetime.now(timezone.utc).isoformat()),
                )

    def _db_get_existing_urls(self, category: str) -> Set[str]:
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            rows = conn.execute(
                "SELECT url FROM businesses WHERE category = ?", (category,)
            ).fetchall()
            return {r[0] for r in rows}

    # ─── Checkpoint ──────────────────────────────────────────────────────

    async def _checkpoint_save(self, slug: str, page: int, urls_found: int, completed: bool = False):
        """Save pagination progress for a category."""
        async with self.db_lock:
            with sqlite3.connect(self.db_path, timeout=15) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO checkpoints "
                    "(category_slug, last_page, urls_found, completed, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (slug, page, urls_found, int(completed),
                     datetime.now(timezone.utc).isoformat()),
                )

    def _checkpoint_get(self, slug: str) -> Optional[dict]:
        """Get checkpoint for a category. Returns None if no checkpoint."""
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM checkpoints WHERE category_slug = ?", (slug,)
            ).fetchone()
            return dict(row) if row else None

    def _checkpoint_summary(self) -> dict:
        """Get summary of checkpoint state."""
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            completed = conn.execute(
                "SELECT COUNT(*) FROM checkpoints WHERE completed = 1"
            ).fetchone()[0]
            in_progress = conn.execute(
                "SELECT COUNT(*) FROM checkpoints WHERE completed = 0"
            ).fetchone()[0]
            return {"completed": completed, "in_progress": in_progress}

    def _checkpoint_clear(self):
        """Clear all checkpoints (--fresh flag)."""
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            conn.execute("DELETE FROM checkpoints")
            conn.commit()

    # ─── Proxy ───────────────────────────────────────────────────────────

    @staticmethod
    def _load_proxies(path: str) -> List[str]:
        proxies = []
        try:
            with open(path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if not line.startswith("http"):
                            line = f"http://{line}"
                        proxies.append(line)
            log.info(f"Loaded {len(proxies)} proxies from {path}")
        except Exception as e:
            log.warning(f"Failed to load proxies from {path}: {e}")
        return proxies

    def _get_proxy(self) -> Optional[dict]:
        if not self.proxy_pool:
            return None
        proxy_url = random.choice(self.proxy_pool)
        return {"http": proxy_url, "https": proxy_url}

    # ─── Fetcher ─────────────────────────────────────────────────────────

    # ─── Session lifecycle ────────────────────────────────────────────

    async def _ensure_session(self):
        """Lazily start the stealth browser session (one browser, page pool)."""
        if self._session is None:
            blocked = {
                "google-analytics.com", "googletagmanager.com",
                "facebook.net", "doubleclick.net", "connect.facebook.net",
                "cdn.ampproject.org", "pagead2.googlesyndication.com",
            }
            session_kwargs = dict(
                headless=True,
                disable_resources=True,
                block_ads=True,
                blocked_domains=blocked,
                timeout=10000,
                max_pages=self.concurrency,
            )
            # Use ProxyRotator if proxies are available (with higher timeout for proxy latency)
            if self.proxy_pool:
                session_kwargs["proxy_rotator"] = ProxyRotator(self.proxy_pool)
                session_kwargs["timeout"] = 20000  # Proxies need more time
                session_kwargs["dns_over_https"] = True  # Prevent DNS leaks through proxy
                log.info(f"[session] ProxyRotator enabled with {len(self.proxy_pool)} proxies")
            self._session = AsyncStealthySession(**session_kwargs)
            await self._session.start()
            log.info("[session] Stealth browser session started (page pool)")

    async def _close_session(self):
        """Close the browser session."""
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None
            log.info("[session] Browser session closed")

    async def _fetch(self, url: str, retries: int = 2, page_action=None):
        """Fetch a page with pooled stealth browser, retry with linear backoff."""
        if self.shutdown_event.is_set():
            return None
        async with self.semaphore:
            await self.delay.wait()
            await self._ensure_session()

            for attempt in range(retries):
                if self.shutdown_event.is_set():
                    return None
                try:
                    fetch_kwargs = dict(
                        url=url,
                        solve_cloudflare=True,
                    )
                    if page_action is not None:
                        fetch_kwargs["page_action"] = page_action
                    page = await self._session.fetch(**fetch_kwargs)
                    # Check for Cloudflare block
                    if page and self._is_blocked(page):
                        log.warning(f"[blocked] CF challenge on attempt {attempt + 1}: {url}")
                        await self.delay.on_failure()
                        await asyncio.sleep(1.5 * (attempt + 1) + random.uniform(0, 1))
                        continue
                    await self.delay.on_success()
                    return page
                except Exception as e:
                    log.warning(f"[fetch] Attempt {attempt + 1}/{retries} failed for {url}: {e}")
                    await self.delay.on_failure()
                    await asyncio.sleep(1.5 * (attempt + 1) + random.uniform(0, 1))

            self.error_count += 1
            log.error(f"[fetch] All {retries} attempts failed: {url}")
            return None

    @staticmethod
    def _is_blocked(page) -> bool:
        status = getattr(page, "status", 200)
        if status >= 400:
            return True
        # Only check body text for CF challenge if content looks suspiciously small
        try:
            body = page.body if hasattr(page, 'body') else b''
            if isinstance(body, str):
                body = body.encode('utf-8', errors='ignore')
            if len(body) < 5000:
                text_nodes = page.css("body *::text").getall()
                body_text = " ".join(t.strip() for t in text_nodes if t.strip())
                if len(body_text) < 100 and "Just a moment" in body_text:
                    return True
        except Exception:
            pass
        return False

    # ─── L0: Category Discovery ──────────────────────────────────────────

    async def discover_categories(self) -> List[dict]:
        """Auto-discover all categories from the business directory."""
        if self.is_kuwait:
            return await self._discover_categories_kuwait()

        url = f"{self.base_url}/business"
        log.info(f"[L0] Discovering categories from {url}")

        page = await self._fetch(url)
        if not page and self.proxy_pool:
            log.warning("[L0] Fetch failed with proxies — retrying direct")
            old_session = self._session
            self._session = None
            saved_pool = self.proxy_pool
            self.proxy_pool = []
            try:
                page = await self._fetch(url)
            finally:
                self.proxy_pool = saved_pool
            if old_session:
                try:
                    await old_session.close()
                except Exception:
                    pass
        if not page:
            log.error("[L0] Could not load business directory page")
            return []

        categories = []
        seen_slugs = set()

        # Get all href values and filter for category links
        all_hrefs = page.css("a[href*='/business/category/']::attr(href)").getall()
        log.info(f"[L0] Found {len(all_hrefs)} raw category hrefs")

        for href in all_hrefs:
            if not href:
                continue
            full_url = urljoin(self.base_url, href)
            path = urlparse(full_url).path
            match = re.search(r"/business/category/([^/?#]+)", path)
            if not match:
                continue
            slug = match.group(1)
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            # Derive readable name from slug
            name = slug.replace("-", " ").replace("%2C", ",").replace("%26", "&").title()
            cat = {"slug": slug, "name": name, "url": full_url}
            categories.append(cat)
            await self._db_insert_category(slug, name, full_url)

        log.info(f"[L0] Found {len(categories)} categories")
        for c in categories:
            log.info(f"  • {c['name']} -> {c['url']}")

        return categories

    async def _discover_categories_kuwait(self) -> List[dict]:
        """Kuwait: categories at /business/categories with /business/categories/SLUG links."""
        url = f"{self.base_url}/business/categories"
        log.info(f"[L0-KW] Discovering categories from {url}")

        page = await self._fetch(url)
        if not page:
            log.error("[L0-KW] Could not load categories page")
            return []

        categories = []
        seen_slugs = set()

        all_hrefs = page.css("a[href*='/business/categories/']::attr(href)").getall()
        log.info(f"[L0-KW] Found {len(all_hrefs)} raw category hrefs")

        for href in all_hrefs:
            if not href:
                continue
            full_url = urljoin(self.base_url, href)
            path = urlparse(full_url).path
            match = re.search(r"/business/categories/([^/?#]+)", path)
            if not match:
                continue
            slug = match.group(1)
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            name = slug.replace("-", " ").replace("%2C", ",").replace("%26", "&").title()
            cat = {"slug": slug, "name": name, "url": full_url}
            categories.append(cat)
            await self._db_insert_category(slug, name, full_url)

        log.info(f"[L0-KW] Found {len(categories)} categories")
        for c in categories:
            log.info(f"  . {c['name']} -> {c['url']}")

        return categories

    # ─── L1: Pagination — Collect business URLs ──────────────────────────

    async def crawl_category_listings(self, cat_url: str, cat_name: str,
                                       queue: asyncio.Queue = None) -> int:
        """Paginate through a category and push business URLs into queue.

        URLs are pushed into the queue as each page is discovered, so consumers
        can start work immediately instead of waiting for full pagination.
        Saves checkpoint per page for resume support.
        Returns the count of new URLs found.
        """
        if self.is_kuwait:
            return await self._kw_crawl_listings(cat_url, cat_name, queue)

        # Check checkpoint for resume
        checkpoint = self._checkpoint_get(cat_name)
        if checkpoint and checkpoint["completed"]:
            log.info(f"[L1] {cat_name} — already completed (checkpoint), skipping")
            return 0

        url_count = 0
        existing = self._db_get_existing_urls(cat_name)
        page_num = 1
        current_url = cat_url

        # Resume from checkpoint if available
        if checkpoint and checkpoint["last_page"] > 1:
            page_num = checkpoint["last_page"] + 1
            url_count = checkpoint["urls_found"]
            current_url = f"{cat_url}/page:{page_num}"
            console.print(f"  [yellow]>> Resuming {cat_name} from page {page_num}[/yellow]")
            log.info(f"[L1] Resuming {cat_name} from page {page_num} ({url_count} URLs already found)")

        while current_url:
            if self.shutdown_event.is_set():
                break
            if self.limit and self.scraped_count >= self.limit:
                log.info(f"[L1] Global limit reached, stopping {cat_name}")
                break

            console.print(f"  [dim]{cat_name}[/dim] page {page_num}…", highlight=False)
            log.info(f"[L1] {cat_name} page {page_num}: {current_url}")
            page = await self._fetch(current_url)
            if not page:
                break

            # Extract business detail links
            links = page.css('a[href*="/business/view/"]::attr(href)').getall()
            new_count = 0
            seen_on_page = set()
            for href in links:
                if not href:
                    continue
                full = urljoin(self.base_url, href)
                path = urlparse(full).path
                if "/business/view/" not in path or full in seen_on_page:
                    continue
                seen_on_page.add(full)
                if full not in existing:
                    new_count += 1
                    url_count += 1
                    # Push URL into queue immediately so consumers can start
                    if queue is not None:
                        # Use timeout to avoid deadlock if consumers exit
                        while not self.shutdown_event.is_set():
                            try:
                                await asyncio.wait_for(queue.put((full, cat_name)), timeout=2.0)
                                break
                            except asyncio.TimeoutError:
                                continue
                        if self.shutdown_event.is_set():
                            break

            log.info(f"[L1] Found {len(seen_on_page)} links, {new_count} new (skipped {len(seen_on_page) - new_count} existing)")

            # Save checkpoint after each page
            await self._checkpoint_save(cat_name, page_num, url_count)

            # Update progress bar total as URLs are discovered
            if new_count and self.progress and self.biz_task_id is not None:
                self.progress.update(
                    self.biz_task_id,
                    total=(self.progress.tasks[self.biz_task_id].total or 0) + new_count,
                )

            if len(seen_on_page) == 0:
                break

            # Find next page — try multiple selectors then fallback to URL pattern
            next_link = (
                page.css('a[rel="next"]::attr(href)').get("")
                or page.css('.pagination .next a::attr(href)').get("")
            )
            if next_link:
                current_url = urljoin(self.base_url, next_link)
            else:
                page_num += 1
                current_url = f"{cat_url}/page:{page_num}"

        # Mark completed if we didn't exit due to shutdown/limit
        if not self.shutdown_event.is_set() and not (self.limit and self.scraped_count >= self.limit):
            await self._checkpoint_save(cat_name, page_num, url_count, completed=True)

        log.info(f"[L1] Total URLs queued for {cat_name}: {url_count}")
        return url_count

    # ─── Kuwait L1: AJAX "Load more" pagination ─────────────────────────

    async def _kw_crawl_listings(self, cat_url: str, cat_name: str,
                                  queue: asyncio.Queue = None) -> int:
        """Kuwait: AJAX 'Load more' pagination via direct HTTP requests."""
        checkpoint = self._checkpoint_get(cat_name)
        if checkpoint and checkpoint["completed"]:
            log.info(f"[L1-KW] {cat_name} — already completed (checkpoint), skipping")
            return 0

        existing = self._db_get_existing_urls(cat_name)

        console.print(f"  [dim]{cat_name}[/dim] loading listings...", highlight=False)
        log.info(f"[L1-KW] {cat_name}: {cat_url}")

        # Step 1: Browser fetch to get initial page + data-total
        page = await self._fetch(cat_url)
        if not page:
            return 0

        gridlist = page.css(".gridlist")
        total = int(gridlist[0].attrib.get("data-total", "0")) if gridlist else 0
        initial_count = len(page.css(".ggrid"))
        log.info(f"[L1-KW] {cat_name}: total={total}, initial={initial_count}")

        # Extract links helper
        kw_skip = {"/categories", "/governorates", "/tags", "/recent"}

        def _extract_biz_links(hrefs):
            links = set()
            for href in hrefs:
                if not href:
                    continue
                full = urljoin(self.base_url, href)
                path = urlparse(full).path
                if not re.match(r"^/business/[^/]+$", path):
                    continue
                if any(skip in path for skip in kw_skip):
                    continue
                links.add(full)
            return links

        # Extract from initial page
        all_hrefs = page.css('a[href]::attr(href)').getall()
        seen = _extract_biz_links(all_hrefs)
        url_count = 0

        for full in seen:
            if full in existing:
                continue
            url_count += 1
            if queue is not None:
                while not self.shutdown_event.is_set():
                    try:
                        await asyncio.wait_for(queue.put((full, cat_name)), timeout=2.0)
                        break
                    except asyncio.TimeoutError:
                        continue
                if self.shutdown_event.is_set():
                    break

        log.info(f"[L1-KW] {cat_name}: page 1 -> {len(seen)} links, {url_count} new")

        # Step 2: AJAX pages via urllib (bypasses Cloudflare; httpx gets blocked)
        if total > initial_count and not self.shutdown_event.is_set():
            offset = initial_count
            ajax_headers = {
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "text/html, */*; q=0.01",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": cat_url,
            }
            page_num = 2
            loop = asyncio.get_event_loop()

            while offset < total and not self.shutdown_event.is_set():
                if self.limit and self.scraped_count >= self.limit:
                    break
                ajax_url = f"{cat_url}?loadmore=yes&offset={offset}&totalrecord={total}"
                try:
                    await self.delay.wait()
                    req = urllib.request.Request(ajax_url, headers=ajax_headers)
                    body = await loop.run_in_executor(
                        None,
                        lambda r=req: urllib.request.urlopen(r, timeout=15).read().decode("utf-8", "replace"),
                    )
                    if not body or body.strip() == "0" or "<!DOCTYPE" in body[:100]:
                        log.info(f"[L1-KW] {cat_name}: AJAX page {page_num} -> empty/end")
                        break

                    # Parse HTML fragment for links
                    from parsel import Selector
                    frag = Selector(text=body)
                    frag_hrefs = frag.css('a[href]::attr(href)').getall()
                    new_links = _extract_biz_links(frag_hrefs) - seen
                    if not new_links:
                        log.info(f"[L1-KW] {cat_name}: AJAX page {page_num} -> 0 new links, stopping")
                        break

                    seen.update(new_links)
                    batch_new = 0
                    for full in new_links:
                        if full in existing:
                            continue
                        batch_new += 1
                        url_count += 1
                        if queue is not None:
                            while not self.shutdown_event.is_set():
                                try:
                                    await asyncio.wait_for(queue.put((full, cat_name)), timeout=2.0)
                                    break
                                except asyncio.TimeoutError:
                                    continue
                            if self.shutdown_event.is_set():
                                break

                    console.print(f"  [dim]{cat_name}[/dim] page {page_num} +{batch_new}", highlight=False)
                    log.info(f"[L1-KW] {cat_name}: AJAX page {page_num} -> {len(new_links)} links, {batch_new} new")
                    await self.delay.on_success()

                    frag_count = len(frag.css(".ggrid"))
                    offset += frag_count if frag_count else len(new_links)
                    page_num += 1

                except Exception as e:
                    log.warning(f"[L1-KW] AJAX page {page_num} failed: {e}")
                    await self.delay.on_failure()
                    break

        # Update progress bar
        if url_count and self.progress and self.biz_task_id is not None:
            self.progress.update(
                self.biz_task_id,
                total=(self.progress.tasks[self.biz_task_id].total or 0) + url_count,
            )

        if not self.shutdown_event.is_set():
            await self._checkpoint_save(cat_name, 1, url_count, completed=True)

        log.info(f"[L1-KW] {cat_name}: TOTAL {url_count} new URLs queued (from {len(seen)} found)")
        return url_count

    # ─── L2: Business Detail Extraction ──────────────────────────────────

    async def scrape_business(self, url: str, category: str):
        """Scrape a single business detail page."""
        if self.limit and self.scraped_count >= self.limit:
            return

        page = await self._fetch(url)
        if not page:
            self.skip_count += 1
            return

        if self.is_kuwait:
            data = self._extract_data_kuwait(page, url)
        else:
            data = self._extract_data(page, url)
        if not data.get("Name"):
            log.warning(f"[L2] No name found, skipping: {url}")
            self.skip_count += 1
            return

        # Save to SQLite
        await self._db_insert_business(url, category, data)

        # Append to raw CSV immediately
        await self._append_raw_csv(data, category, url)

        self.scraped_count += 1
        log.info(f"[L2] #{self.scraped_count} {data['Name']} — {url}")

        # Signal shutdown if limit reached
        if self.limit and self.scraped_count >= self.limit:
            if not self.shutdown_event.is_set():
                console.print(
                    Panel.fit(
                        f"[bold green]OK Limit of {self.limit} reached[/bold green] -- "
                        f"draining in-flight requests...",
                        border_style="green",
                    )
                )
                log.info(f"[limit] Reached {self.scraped_count}/{self.limit} — shutting down producers")
            self.shutdown_event.set()

    def _extract_data(self, page, url: str) -> Dict[str, str]:
        """Extract all business fields from a detail page."""
        text_nodes = page.css("body *::text").getall()
        full_text = "\n".join(t.strip() for t in text_nodes if t.strip())

        try:
            html = page.body.decode(
                getattr(page, "encoding", None) or "utf-8", errors="replace"
            )
        except Exception:
            html = ""

        data = {}

        # Name
        data["Name"] = self._extract_name(page)

        # Phone: collect all numbers, split into Phone_1/2/3
        phones_raw = []
        tel_links = page.css('a[href^="tel:"]::attr(href)').getall()
        for t in tel_links:
            num = t.replace("tel:", "").strip()
            if num:
                phones_raw.append(num)

        if not phones_raw:
            # Combined regex: prefix + optional separator + digits, or spaced format
            prefix_escaped = re.escape(self.phone_prefix)
            phones_raw = re.findall(
                rf"{prefix_escaped}[\s-]?\d{{1,3}}[\s-]?\d{{4,9}}(?!\d)", full_text
            )

        # Deduplicate and assign to Phone_1, Phone_2, Phone_3
        seen_p = set()
        cleaned_phones = []
        for p in phones_raw:
            c = re.sub(r"\s+", " ", p).strip()
            if c not in seen_p:
                seen_p.add(c)
                cleaned_phones.append(c)
        for i, phone in enumerate(cleaned_phones[:3], 1):
            data[f"Phone_{i}"] = phone

        # WhatsApp
        wa_links = page.css('a[href*="wa.me"]::attr(href)').getall()
        for wa in wa_links:
            m_wa = re.search(r'wa\.me/(\d+)', wa)
            if m_wa:
                num = f"+{m_wa.group(1)}"
                data["WhatsApp"] = num
                break

        # Email
        m = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", full_text)
        if m:
            email = m.group(0)
            if "." in email.split("@")[1]:
                data["Email"] = email

        # Location fields: labelled first (UAE/KW), CSS fallback (SA)
        loc = self._extract_labelled(full_text, "Address")
        if not loc:
            locs = page.css('.bus_addr span::text, .bus_addr a::text, .location::text').getall()
            seen_loc = []
            for l in locs:
                l = l.strip().rstrip(",")
                if l and not l.isspace() and l not in seen_loc:
                    seen_loc.append(l)
            loc = ", ".join(seen_loc)
            loc = re.sub(r",\s*,", ",", loc).strip(", ")
        data["Location"] = loc

        area = self._extract_labelled(full_text, "Area")
        gov = self._extract_labelled(full_text, "Governorate").rstrip(",").strip()

        # For SA pages: parse structured CSS location into Area/Governorate if labelled failed
        if not area and not gov and loc:
            parts = [p.strip() for p in loc.split(",") if p.strip()]
            if len(parts) >= 3:
                area = parts[0]       # e.g., "Riyadh City"
                gov = parts[-2]       # e.g., "Riyadh Province"
            elif len(parts) == 2:
                area = parts[0]

        data["Area"] = area
        data["Governorate"] = gov

        country = self._extract_labelled(full_text, "Country")
        # Country should be a short name, not page overflow
        if country:
            country = country.split("\n")[0].strip()
            if len(country) > 50:
                country = country[:50].rsplit(" ", 1)[0]
        data["Country"] = country or self.country_name

        # Fax — arablocal uses span.hot_icon inside div.bus_addr for fax
        for div in page.css("div.bus_addr"):
            if div.css("span.hot_icon"):
                num = div.css("p::text").get("").strip()
                if num and re.match(r"^[\d\s+()-]+$", num):
                    data["Fax"] = num
                    break
        if "Fax" not in data:
            m = re.search(r"(?:fax|Fax|FAX)\s*[:\s]*([\d\s+()-]+)", full_text)
            if m:
                data["Fax"] = m.group(1).strip()

        # Rating
        no_rating = page.css("#no_business_comment::text").get("")
        if "No ratings" not in no_rating:
            stars = page.css(".bus_star .star_icon")
            if stars:
                filled = len([s for s in stars if "active" in s.attrib.get("class", "")])
                if filled > 0:
                    data["Rating"] = str(filled)
            if "Rating" not in data:
                m = re.search(r"(\d+(?:\.\d+)?)\s*(?:out of\s*\d+|/\s*5)", full_text)
                if m:
                    data["Rating"] = m.group(1)

        # Views: fa-eye regex on raw HTML (arablocal pattern), CSS fallback
        if html:
            m_views = re.search(r'fa-eye.*?>\s*([\d,]+)\s*<', html)
            if m_views:
                data["Views"] = m_views.group(1).strip().replace(",", "")
        if "Views" not in data:
            view_texts = page.css("span.bus_view *::text").getall()
            for t in view_texts:
                t = t.strip().replace(",", "")
                if t.isdigit():
                    data["Views"] = t
                    break

        # About (truncate to 500 chars for clean CSV)
        parts = page.css("div.bus_inner_content_desc *::text").getall()
        about = " ".join(t.strip() for t in parts if t.strip())
        if not about or len(about) <= 10:
            about = page.css("meta[property='og:description']::attr(content)").get("").strip()
        if about and len(about) > 10:
            about = re.sub(r"\s+", " ", about).strip()
            if len(about) > 500:
                about = about[:497] + "..."
            data["About"] = about

        # Website: dedicated link first (most reliable)
        bus_url = page.css("a.bus_url_a::attr(href)").get("").strip()
        if bus_url:
            data["Website"] = bus_url

        # Scan links ONLY inside div.bus_inner (business content area)
        # to avoid picking up site-wide footer/header/sidebar links
        bus_inner = page.css("div.bus_inner")
        link_scope = bus_inner[0].css("a[href]") if bus_inner else []
        for a in link_scope:
            href = (a.css("::attr(href)").get("") or "").strip()
            if not href or not href.startswith("http"):
                continue
            domain = urlparse(href).netloc.lower()

            # Skip arablocal's own links
            if "arablocal" in href.lower():
                continue
            # Skip known site-owned social accounts
            if any(acct in href for acct in SITE_SOCIAL_BLACKLIST):
                continue

            # Check social domains
            is_social = False
            for soc_domain, platform in SOCIAL_DOMAINS.items():
                if soc_domain in domain:
                    if platform not in data:
                        data[platform] = href
                    is_social = True
                    break

            # Website: first external non-social, non-platform link
            if not is_social and "Website" not in data:
                if not any(ign in domain for ign in IGNORE_FOR_WEBSITE):
                    data["Website"] = href

        # Clean all values
        return {k: self._clean(v) for k, v in data.items() if v}

    def _extract_data_kuwait(self, page, url: str) -> Dict[str, str]:
        """Kuwait-specific extraction — different DOM structure from ArabLocal."""
        text_nodes = page.css("body *::text").getall()
        full_text = "\n".join(t.strip() for t in text_nodes if t.strip())

        data = {}

        # Name: h2 heading, strip "(Open)" / "(Closed)" suffix
        h2 = page.css("h2::text").get("").strip()
        if h2:
            h2 = re.sub(r"\s*\(\s*(?:Open|Closed)\s*\)\s*$", "", h2, flags=re.I).strip()
        if not h2 or len(h2) <= 2:
            title = page.css("title::text").get("").strip()
            h2 = re.sub(
                r"\s*[|\-\u2013]\s*(?:Kuwait).*$", "", title, flags=re.I
            ).strip()
        data["Name"] = h2

        # Phone: from tel: links + text regex
        phones_raw = []
        tel_links = page.css('a[href^="tel:"]::attr(href)').getall()
        for t in tel_links:
            num = t.replace("tel:", "").strip()
            if num:
                phones_raw.append(num)

        if not phones_raw:
            phones_raw = re.findall(r"\+965[\s-]?\d{7,9}(?!\d)", full_text)
            if not phones_raw:
                phones_raw = re.findall(r"\+965\s?\d{1,3}[\s-]\d{4,8}(?!\d)", full_text)

        seen_p = set()
        cleaned_phones = []
        for p in phones_raw:
            c = re.sub(r"\s+", " ", p).strip()
            if c not in seen_p:
                seen_p.add(c)
                cleaned_phones.append(c)
        for i, phone in enumerate(cleaned_phones[:3], 1):
            data[f"Phone_{i}"] = phone

        # WhatsApp
        wa_links = page.css('a[href*="wa.me"]::attr(href)').getall()
        for wa in wa_links:
            m_wa = re.search(r'wa\.me/(\d+)', wa)
            if m_wa:
                data["WhatsApp"] = f"+{m_wa.group(1)}"
                break

        # Email
        m = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", full_text)
        if m:
            email = m.group(0)
            if "." in email.split("@")[1]:
                data["Email"] = email

        # Address: inline text before governorate link
        # Kuwait format: "Block: 8, Building: 8, Street: 37, ,Fahaheel, Governorate,Kuwait."
        # Try to extract from page text near the heading
        addr_parts = []
        gov_link = page.css('a[href*="/business/governorates/"]')
        gov_name = ""
        if gov_link:
            gov_name = gov_link[0].css("::text").get("").strip()
            data["Governorate"] = gov_name

        # Try extracting address from text between name and governorate
        if gov_name:
            name_idx = full_text.find(data.get("Name", ""))
            gov_idx = full_text.find(gov_name)
            if name_idx >= 0 and gov_idx > name_idx:
                addr_block = full_text[name_idx + len(data.get("Name", "")):gov_idx].strip()
                # Clean up the address block
                addr_block = re.sub(r"^\s*\(\s*(?:Open|Closed)\s*\)\s*", "", addr_block)
                addr_block = re.sub(r"\s+", " ", addr_block).strip().strip(",").strip()
                if addr_block and len(addr_block) > 3:
                    data["Location"] = addr_block

        data["Country"] = "Kuwait"

        # About / Description
        # Kuwait uses a "Description" section
        desc_parts = []
        in_desc = False
        for node in text_nodes:
            t = node.strip()
            if t == "Description":
                in_desc = True
                continue
            if in_desc:
                # Stop at next section header
                if t in ("Categories :", "Tags :", "Gallery", "Services",
                          "Menu", "Regular Working Hours", "Products Services"):
                    break
                if t and len(t) > 2:
                    desc_parts.append(t)
        about = " ".join(desc_parts).strip()
        if about:
            about = re.sub(r"\s+", " ", about).strip()
            if len(about) > 500:
                about = about[:497] + "..."
            if len(about) > 10:
                data["About"] = about

        # Website: external links (not kuwaitlocal.com, not social)
        all_links = page.css('a[href]')
        for a in all_links:
            href = (a.css("::attr(href)").get("") or "").strip()
            if not href or not href.startswith("http"):
                continue
            domain = urlparse(href).netloc.lower()
            if any(acct in href for acct in SITE_SOCIAL_BLACKLIST):
                continue

            # Social links
            is_social = False
            for soc_domain, platform in SOCIAL_DOMAINS.items():
                if soc_domain in domain:
                    if platform not in data:
                        data[platform] = href
                    is_social = True
                    break

            if not is_social and "Website" not in data:
                if not any(ign in domain for ign in IGNORE_FOR_WEBSITE):
                    data["Website"] = href

        # Clean all values
        return {k: self._clean(v) for k, v in data.items() if v}

    # ─── Extraction Helpers ──────────────────────────────────────────────

    @staticmethod
    def _extract_name(page) -> str:
        for h3 in page.css("h3"):
            cls = h3.attrib.get("class", "")
            if "font_size_22" in cls:
                val = h3.css("::text").get("").strip()
                if val and len(val) > 2:
                    return val
        # Fallback: h1
        h1 = page.css("h1::text").get("").strip()
        if h1 and len(h1) > 2:
            return h1
        # Fallback: title
        title = page.css("title::text").get("").strip()
        if title:
            # Strip country suffixes: "Business | Oman | Oman Local" or "Business - UAE Local"
            title = re.sub(
                r"\s*[|\-\u2013]\s*(?:UAE|Kuwait|Saudi|Bahrain|Qatar|Oman).*$",
                "", title, flags=re.I
            ).strip()
            if title and len(title) > 2:
                return title
        return ""

    @staticmethod
    def _extract_labelled(text: str, label: str) -> str:
        labels = r"Address|Area|Governorate|Country|Landmark|P\.?O\.?"
        pattern = rf"{label}\s*:\s*(.+?)(?=(?:{labels})\s*:|[\n])"
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            val = re.sub(r"\s+", " ", m.group(1)).strip()
            # Cap at 200 chars to avoid page-content overflow
            if len(val) > 200:
                val = val[:200].rsplit(" ", 1)[0]
            return val
        return ""

    @staticmethod
    def _clean(text) -> str:
        if not text:
            return ""
        text = str(text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
        return re.sub(r"\s+", " ", text).strip()

    # ─── Raw CSV (batched, flush every N rows) ───────────────────────

    async def _append_raw_csv(self, data: dict, category: str, url: str):
        """Buffer a business row and flush to CSV when buffer is full."""
        async with self.csv_lock:
            try:
                elapsed = time.monotonic() - self.start_time if self.start_time else 0
                row = {
                    "Category": category,
                    "URL": url,
                    "Scraped_At": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "Runtime_Sec": f"{elapsed:.0f}",
                }
                all_keys = list(BASE_FIELDS) + sorted(
                    k for k in data if k not in BASE_FIELDS
                )
                row.update({k: data.get(k, "") for k in all_keys})
                self._csv_buffer.append(row)

                # Recompute fieldnames only when new keys appear
                fieldnames = ["Category", "URL", "Scraped_At", "Runtime_Sec"] + all_keys
                if self._csv_fieldnames is None or set(fieldnames) != set(self._csv_fieldnames):
                    self._csv_fieldnames = fieldnames

                if len(self._csv_buffer) >= self._csv_flush_size:
                    self._flush_csv_buffer()
            except Exception as e:
                log.warning(f"Raw CSV buffer failed: {e}")

    def _flush_csv_buffer(self):
        """Write buffered rows to CSV file (call under csv_lock)."""
        if not self._csv_buffer or not self._csv_fieldnames:
            return
        try:
            file_exists = os.path.exists(RAW_CSV)
            with open(RAW_CSV, "a", newline="", encoding="utf-8-sig") as fh:
                writer = csv.DictWriter(fh, fieldnames=self._csv_fieldnames, extrasaction="ignore")
                if not file_exists:
                    writer.writeheader()
                writer.writerows(self._csv_buffer)
            self._csv_buffer.clear()
        except Exception as e:
            log.warning(f"Raw CSV flush failed: {e}")

    async def flush_csv(self):
        """Force-flush remaining CSV buffer (call at shutdown)."""
        async with self.csv_lock:
            self._flush_csv_buffer()

    # ─── Pipeline ────────────────────────────────────────────────────────

    async def run_pipeline(self):
        """Execute the full L0 -> L1 -> L2 producer-consumer pipeline."""
        self.start_time = time.monotonic()

        # Handle --fresh flag
        if getattr(self.args, "fresh", False):
            self._checkpoint_clear()
            console.print("[yellow]--fresh: Checkpoints cleared, starting from scratch[/yellow]")

        # L0: Discover categories
        console.print("[bold blue]Discovering categories…[/bold blue]")
        categories = await self.discover_categories()
        if not categories:
            console.print("[red]No categories discovered. Exiting.[/red]")
            return

        # Filter by --category if specified
        if self.args.category:
            target = self.args.category.lower()
            categories = [c for c in categories if c["slug"].lower() == target]
            if not categories:
                console.print(f"[red]Category '{self.args.category}' not found[/red]")
                return

        console.print(f"[green]{len(categories)} categories ready[/green]")

        # Show checkpoint resume info
        cp_summary = self._checkpoint_summary()
        if cp_summary["completed"] or cp_summary["in_progress"]:
            console.print(
                f"[cyan]>> Checkpoint: {cp_summary['completed']} categories complete, "
                f"{cp_summary['in_progress']} in-progress — resuming[/cyan]"
            )
            log.info(f"[checkpoint] Resuming: {cp_summary['completed']} complete, "
                     f"{cp_summary['in_progress']} in-progress")

        if self.args.dry_run:
            console.print("[yellow][dry-run] Category discovery complete. Exiting.[/yellow]")
            return

        # ── Producer-consumer with Rich progress ─────────────────────────
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.concurrency * 10)
        cats_completed = 0
        total_cats = len(categories)
        quiet = getattr(self.args, "quiet", False)
        producer_tasks: List[asyncio.Task] = []
        consumer_tasks: List[asyncio.Task] = []

        # Separate semaphore for L1 — allow 4 categories to paginate at once
        # so if one is slow/blocked, others can still feed the queue
        l1_semaphore = asyncio.Semaphore(4)

        progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=console,
            transient=False,
            disable=quiet,
        )
        progress.start()
        self.progress = progress
        self.cat_task_id = progress.add_task("[bold]Categories[/bold]", total=total_cats)
        self.biz_task_id = progress.add_task("[bold]Businesses[/bold]", total=0)

        try:
            # L1 producer: paginate category → stream URLs directly into queue
            async def l1_producer(cat):
                nonlocal cats_completed
                if self.shutdown_event.is_set() or (self.limit and self.scraped_count >= self.limit):
                    cats_completed += 1
                    progress.update(self.cat_task_id, completed=cats_completed)
                    return
                async with l1_semaphore:
                    if self.shutdown_event.is_set():
                        cats_completed += 1
                        progress.update(self.cat_task_id, completed=cats_completed)
                        return
                    try:
                        # URLs are pushed into queue per-page inside crawl_category_listings
                        await self.crawl_category_listings(
                            cat["url"], cat["slug"], queue=queue
                        )
                    except asyncio.CancelledError:
                        return
                    finally:
                        cats_completed += 1
                        progress.update(self.cat_task_id, completed=cats_completed)

            # L2 consumer: pull URLs and scrape
            async def l2_consumer():
                while not self.shutdown_event.is_set():
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=2.0)
                    except asyncio.TimeoutError:
                        continue
                    except asyncio.CancelledError:
                        return
                    if item is _SENTINEL:
                        queue.task_done()
                        break
                    url, category = item
                    if not (self.limit and self.scraped_count >= self.limit):
                        await self.scrape_business(url, category)
                        progress.update(self.biz_task_id, advance=1)
                    queue.task_done()

            # Start L2 consumers first so they're ready when URLs arrive
            consumer_tasks = [
                asyncio.create_task(l2_consumer())
                for _ in range(self.concurrency)
            ]
            # Start L1 producers (l1_semaphore serializes them so consumers get semaphore slots)
            producer_tasks = [asyncio.create_task(l1_producer(cat)) for cat in categories]

            # Wait for all producers to finish
            await asyncio.gather(*producer_tasks, return_exceptions=True)

            if self.shutdown_event.is_set():
                # Consumers already exited via shutdown_event check —
                # drain leftover queue items so we don't block on sentinel push
                while not queue.empty():
                    try:
                        queue.get_nowait()
                        queue.task_done()
                    except asyncio.QueueEmpty:
                        break
            else:
                # Normal finish: signal consumers with sentinels
                for _ in consumer_tasks:
                    await queue.put(_SENTINEL)

            # Wait for all consumers to complete
            await asyncio.gather(*consumer_tasks, return_exceptions=True)

        except (asyncio.CancelledError, KeyboardInterrupt):
            console.print(
                f"\n[yellow][interrupted] Ctrl+C detected — finishing {self.concurrency} "
                f"in-flight tasks…[/yellow]"
            )
            log.warning(f"[interrupted] Pipeline interrupted — {self.scraped_count} scraped so far")
            self.shutdown_event.set()
            for t in producer_tasks:
                t.cancel()
            for _ in consumer_tasks:
                try:
                    queue.put_nowait(_SENTINEL)
                except asyncio.QueueFull:
                    pass
            for t in consumer_tasks:
                t.cancel()
            # Wait briefly for tasks to acknowledge cancellation
            all_tasks = producer_tasks + consumer_tasks
            if all_tasks:
                await asyncio.gather(*all_tasks, return_exceptions=True)
        finally:
            await self.flush_csv()
            progress.stop()
            self.progress = None
            await self._close_session()

        console.print(f"[bold green][done] Pipeline complete — {self.scraped_count} businesses "
                       f"in {time.monotonic() - self.start_time:.0f}s[/bold green]")
        log.info(f"[done] Pipeline complete. Total scraped: {self.scraped_count}")

    # ─── Export: Priority-Sorted CSVs ────────────────────────────────────

    def export_sorted_csvs(self):
        """Export per-category CSVs with priority sorting from SQLite."""
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            conn.row_factory = sqlite3.Row

            categories = [
                row["category"]
                for row in conn.execute("SELECT DISTINCT category FROM businesses")
            ]

            if not categories:
                log.warning("No data in database to export.")
                return

            social_keys = set(SOCIAL_DOMAINS.values())

            for cat in categories:
                rows = conn.execute(
                    "SELECT url, data FROM businesses WHERE category = ?", (cat,)
                ).fetchall()

                records = []
                all_keys: Set[str] = set()
                for row in rows:
                    d = json.loads(row["data"])
                    d["URL"] = row["url"]
                    all_keys.update(d.keys())
                    records.append(d)

                if not records:
                    continue

                # Build fieldnames: base first, then sorted extras
                base_order = ["Name", "Phone_1", "Phone_2", "Phone_3", "WhatsApp",
                              "Email", "Location", "Area",
                              "Governorate", "Country", "About", "Website",
                              "Rating", "Views", "Fax", "URL"]
                fieldnames = [c for c in base_order if c in all_keys]
                fieldnames += sorted(k for k in all_keys if k not in base_order)

                # Priority sort
                def sort_tier(r):
                    has_web = bool(r.get("Website"))
                    has_soc = any(r.get(k) for k in social_keys)
                    if not has_web and not has_soc:
                        return 1  # Highest priority: no online presence
                    if not has_web and has_soc:
                        return 2  # Has social but no website
                    return 3      # Has website

                records.sort(key=sort_tier)

                safe_name = re.sub(r"[^a-z0-9]+", "_", cat.lower()).strip("_")
                filename = f"{safe_name}.csv"

                with open(filename, "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for rec in records:
                        writer.writerow({col: rec.get(col, "") for col in fieldnames})

                log.info(f"Exported {len(records)} records -> {filename} (priority sorted)")

            # Also export combined file
            all_rows = conn.execute("SELECT url, category, data FROM businesses").fetchall()
            if all_rows:
                all_records = []
                all_keys_combined: Set[str] = set()
                for row in all_rows:
                    d = json.loads(row["data"])
                    d["URL"] = row["url"]
                    d["Category"] = row["category"]
                    all_keys_combined.update(d.keys())
                    all_records.append(d)

                combined_order = ["Category"] + [c for c in base_order if c in all_keys_combined]
                combined_order += sorted(k for k in all_keys_combined if k not in combined_order)

                with open("arablocal_all.csv", "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=combined_order)
                    writer.writeheader()
                    for rec in all_records:
                        writer.writerow({col: rec.get(col, "") for col in combined_order})

                log.info(f"Exported {len(all_records)} total records -> arablocal_all.csv")

    # ─── Summary Report ──────────────────────────────────────────────────

    def print_summary(self):
        """Print a Rich summary table at the end of the run."""
        elapsed = time.monotonic() - self.start_time if self.start_time else 0
        rate = (self.scraped_count / (elapsed / 60)) if elapsed > 60 else self.scraped_count

        table = Table(title="Run Summary", show_header=False, border_style="blue")
        table.add_column("Metric", style="bold")
        table.add_column("Value", style="green")

        table.add_row("Duration", f"{elapsed:.0f}s ({elapsed / 60:.1f}m)")
        table.add_row("Businesses scraped", str(self.scraped_count))
        table.add_row("Skipped / errors", f"{self.skip_count} / {self.error_count}")
        table.add_row("Speed", f"{rate:.1f} / min")
        table.add_row("Concurrency", str(self.concurrency))
        table.add_row("Adaptive delay", f"{self.delay.current:.2f}s")

        with sqlite3.connect(self.db_path, timeout=15) as conn:
            total_db = conn.execute("SELECT COUNT(*) FROM businesses").fetchone()[0]
            total_cats = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        table.add_row("DB records (total)", str(total_db))
        table.add_row("Categories in DB", str(total_cats))

        if os.path.exists(RAW_CSV):
            table.add_row("Raw CSV", RAW_CSV)

        console.print()
        console.print(table)


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="ArabLocal Universal Scraper v2 — SQLite staged, CLI driven",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper_v2.py --dry-run                    # Discover categories only
  python scraper_v2.py --category health-care --limit 5  # Test: 5 businesses from health-care
  python scraper_v2.py --category health-care       # Full health-care category
  python scraper_v2.py                              # ALL categories
  python scraper_v2.py --export-only                # Re-export CSVs from DB
  python scraper_v2.py --proxies proxies.txt        # Use proxy list
  python scraper_v2.py --country kw                 # Target Kuwait
        """,
    )
    parser.add_argument(
        "--category", "-c", type=str, default=None,
        help="Scrape only this category slug (e.g. health-care, restaurants)",
    )
    parser.add_argument(
        "--limit", "-l", type=int, default=0,
        help="Max number of business detail pages to scrape (0 = unlimited)",
    )
    parser.add_argument(
        "--concurrency", "-n", type=int, default=0,
        help="Override max concurrent browser tabs (default: auto, max 5)",
    )
    parser.add_argument(
        "--proxies", "-p", type=str, default=None,
        help="Path to proxy list file (one per line: ip:port or http://ip:port)",
    )
    parser.add_argument(
        "--country", type=str, default="uae",
        choices=list(BASE_URLS.keys()),
        help="Target country subdomain (default: uae)",
    )
    parser.add_argument(
        "--export-only", "-e", action="store_true",
        help="Skip scraping, just export CSVs from existing database",
    )
    parser.add_argument(
        "--dry-run", "-d", action="store_true",
        help="Discover categories only, don't scrape business pages",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Show detailed log output on console (debug level)",
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress progress bar, show only errors and final summary",
    )
    parser.add_argument(
        "--fresh", action="store_true",
        help="Ignore checkpoints and start from scratch (keeps existing DB data)",
    )
    return parser.parse_args()


async def async_main(args):
    # ── Configure console logging verbosity ──
    if args.verbose:
        log.addHandler(RichHandler(console=console, level=logging.DEBUG, show_path=False))
    elif not args.quiet:
        log.addHandler(RichHandler(console=console, level=logging.WARNING, show_path=False))

    # Suppress Scrapling's internal INFO noise in non-verbose mode
    if not args.verbose:
        logging.getLogger("scrapling").setLevel(logging.WARNING)

    engine = ArabLocalEngine(args)

    if args.export_only:
        engine.export_sorted_csvs()
        engine.print_summary()
        return

    try:
        await engine.run_pipeline()
    except (asyncio.CancelledError, KeyboardInterrupt):
        log.warning("Pipeline interrupted.")
        engine.shutdown_event.set()
    finally:
        try:
            console.print("[bold][saving] Exporting final CSVs…[/bold]")
            log.info("[saving] Writing final CSV exports…")
            engine.export_sorted_csvs()
            engine.print_summary()
        except Exception as e:
            log.error(f"Export failed: {e}")


def main():
    args = parse_args()

    mode = "export-only" if args.export_only else "dry-run" if args.dry_run else "full"
    console.print(
        Panel.fit(
            f"[bold]Country:[/bold] {args.country}  "
            f"[bold]Category:[/bold] {args.category or 'ALL'}  "
            f"[bold]Limit:[/bold] {args.limit or 'unlimited'}  "
            f"[bold]Mode:[/bold] {mode}",
            title="[bold blue]ArabLocal Scraper v2[/bold blue]",
            border_style="blue",
        )
    )

    log.info(f"Start: country={args.country}, category={args.category or 'ALL'}, "
             f"limit={args.limit or 'unlimited'}, mode={mode}")

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        console.print("\n[yellow][interrupted] Data saved to SQLite + raw CSV. "
                      "Run again to resume, or --export-only to re-export.[/yellow]")


if __name__ == "__main__":
    main()
