"""ArabLocalEngine — stealth browser session, fetch, pipeline orchestration."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from scrapling.fetchers import AsyncStealthySession, ProxyRotator

from core.config import JobConfig, _SENTINEL
from core.delay import AdaptiveDelay
from core.discovery import crawl_category_listings, discover_categories
from core.extraction import extract_data, extract_data_kuwait
from core.storage import StorageManager

log = logging.getLogger("arablocal")
console = Console(quiet=True)

# Countries where headful mode is required (Cloudflare Turnstile detected).
# Populated at runtime; survives across engine instances within the same process.
_headful_countries: set = set()


class ArabLocalEngine:
    """Core scraper engine: stealth browser, fetch logic, pipeline orchestration.

    Each engine instance handles one country (one JobConfig).
    Multiple engines can run in parallel for multi-country scraping.
    """

    def __init__(self, job: JobConfig, concurrency: int,
                 proxies: Optional[List[str]] = None,
                 cookies: Optional[List[dict]] = None):
        self.job = job
        self.concurrency = concurrency
        self.semaphore = asyncio.Semaphore(concurrency)
        self.limit = job.limit

        # Stealth browser session (pooled — one browser, many pages)
        self._session: Optional[AsyncStealthySession] = None
        self._session_is_headful: bool = False

        # Pre-solved cookies from CookieManager (injected after session start)
        self._cookies: Optional[List[dict]] = cookies
        self._cookies_injected: bool = False

        # Headful mode: required when Cloudflare Turnstile blocks headless browsers
        # If cookies are provided, start headless (cookies bypass CF)
        self._headful_mode: bool = (
            job.country_key in _headful_countries and not cookies
        )

        # Proxy pool
        self.proxy_pool: List[str] = list(proxies) if proxies else []

        # Counters & stats
        self.scraped_count = 0
        self.skip_count = 0
        self.error_count = 0
        self.start_time: float = 0.0
        self.delay = AdaptiveDelay()

        # Shutdown flag
        self.shutdown_event = asyncio.Event()

        # Category progress callback: (cat_name, page_num, total_pages, new_urls)
        self.category_progress_callback = None

        # CF detection callback: called when fetch() detects a CF challenge mid-scrape
        # Signature: async callback(engine) → bool (True if cookies refreshed)
        self.on_cf_detected_callback = None

        # Categories discovered callback: called after L0 discovery completes
        # Signature: callback(categories: list[dict])
        self.categories_discovered_callback = None

        # Rich progress (set during pipeline)
        self.progress: Optional[Progress] = None
        self.cat_task_id = None
        self.biz_task_id = None

        # Storage
        self.storage = StorageManager(
            db_path=job.db_path,
            raw_csv_path=job.raw_csv_path,
            categories_dir=job.categories_dir,
            all_csv_path=job.all_csv_path,
            start_time_ref=lambda: self.start_time,
        )

        log.info(
            f"Engine [{job.country_key}] initialized: base={job.base_url}, "
            f"concurrency={concurrency}, proxies={len(self.proxy_pool)}, "
            f"limit={self.limit or 'unlimited'}"
        )

    # ─── Proxy ───────────────────────────────────────────────────────────

    @staticmethod
    def load_proxies(path: str) -> List[str]:
        """Load proxy list from a file. One proxy per line."""
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

    # ─── Session lifecycle ───────────────────────────────────────────────

    async def _ensure_session(self):
        """Lazily start the stealth browser session (one browser, page pool).

        If the browser executable is missing, attempts a one-time auto-install.
        Restarts the session if headful mode was enabled after session creation.
        """
        # If headful mode was enabled after session was created, restart
        if (self._session is not None
                and self._headful_mode
                and not self._session_is_headful):
            log.info("[session] Closing headless session — headful mode now required")
            await self._close_session()

        if self._session is None:
            # Silence scrapling's internal "Fetched (200)" logs
            _scrapling_log = logging.getLogger("scrapling")
            _scrapling_log.setLevel(logging.CRITICAL)
            _scrapling_log.propagate = False

            blocked = {
                "google-analytics.com", "googletagmanager.com",
                "facebook.net", "doubleclick.net", "connect.facebook.net",
                "cdn.ampproject.org", "pagead2.googlesyndication.com",
            }
            session_kwargs = dict(
                headless=not self._headful_mode,
                timeout=45000 if self._headful_mode else 30000,
                max_pages=self.concurrency,
            )
            # In headful mode, don't block resources — Turnstile needs them
            if not self._headful_mode:
                session_kwargs["disable_resources"] = True
                session_kwargs["block_ads"] = True
                session_kwargs["blocked_domains"] = blocked
            if self.proxy_pool:
                session_kwargs["proxy_rotator"] = ProxyRotator(self.proxy_pool)
                session_kwargs["timeout"] = 40000
                session_kwargs["dns_over_https"] = True
                log.info(f"[session] ProxyRotator enabled with {len(self.proxy_pool)} proxies")
            self._session = AsyncStealthySession(**session_kwargs)

            try:
                await self._session.start()
            except Exception as exc:
                self._session = None
                err_msg = str(exc)
                if "Executable doesn't exist" in err_msg or "browsertype.launch" in err_msg.lower():
                    log.warning("[session] Browser not found — attempting auto-install...")
                    from gui.bootstrap import _install_browser_cli
                    ok = _install_browser_cli()
                    if ok:
                        log.info("[session] Browser installed — retrying session start...")
                        self._session = AsyncStealthySession(**session_kwargs)
                        await self._session.start()
                    else:
                        raise RuntimeError(
                            "Chromium browser not found and auto-install failed. "
                            "Run: python -m patchright install chromium"
                        ) from exc
                else:
                    raise

            log.info("[session] Stealth browser session started (page pool)")
            self._session_is_headful = self._headful_mode

            # Inject pre-solved cookies into the session context
            if self._cookies and not self._cookies_injected:
                try:
                    await self._session.context.add_cookies(self._cookies)
                    self._cookies_injected = True
                    log.info(
                        f"[session] Injected {len(self._cookies)} cookies "
                        f"for {self.job.country_key}"
                    )
                except Exception as e:
                    log.warning(f"[session] Cookie injection failed: {e}")

    async def _close_session(self):
        """Close the browser session."""
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None
            log.info("[session] Browser session closed")

    def set_headful_mode(self):
        """Enable headful mode for this engine and remember it for future instances."""
        if not self._headful_mode:
            self._headful_mode = True
            _headful_countries.add(self.job.country_key)
            log.info(
                f"[session] Headful mode enabled for {self.job.country_key} "
                "(Cloudflare Turnstile detected)"
            )

    async def _restart_as_headful(self):
        """Close the current session and restart in headful mode."""
        self.set_headful_mode()
        await self._close_session()
        await self._ensure_session()

    async def reinject_cookies(self, cookies: List[dict]):
        """Re-inject fresh cookies into the existing session (mid-scrape refresh)."""
        self._cookies = cookies
        self._cookies_injected = False
        if self._session:
            try:
                await self._session.context.add_cookies(cookies)
                self._cookies_injected = True
                log.info(f"[session] Re-injected {len(cookies)} cookies for {self.job.country_key}")
            except Exception as e:
                log.warning(f"[session] Cookie re-injection failed: {e}")

    @staticmethod
    def is_cf_challenge(page) -> bool:
        """Check if a fetched page is a Cloudflare challenge (not real content)."""
        if page is None:
            return False
        title = page.css("title::text").get("") or ""
        if "just a moment" in title.lower():
            return True
        # Also detect by checking for turnstile iframe
        if page.css("iframe[src*='challenges.cloudflare.com']"):
            return True
        return False

    # ─── Fetcher ─────────────────────────────────────────────────────────

    async def fetch(self, url: str, retries: int = 2, page_action=None):
        """Fetch a page with pooled stealth browser, retry with linear backoff.

        In headless mode with cookies, CF is bypassed. If CF is detected mid-scrape
        (cookie expired), calls on_cf_detected_callback to refresh cookies and retries.
        In headful mode, CF solving is enabled and works automatically.
        """
        if self.shutdown_event.is_set():
            return None
        async with self.semaphore:
            await self.delay.wait()
            await self._ensure_session()

            for attempt in range(retries):
                if self.shutdown_event.is_set():
                    return None
                try:
                    fetch_kwargs = dict(url=url)

                    # Only use solve_cloudflare in headful mode — headless CF
                    # solving loops forever on Turnstile "managed" challenges.
                    if self.job.needs_cloudflare and self._headful_mode:
                        fetch_kwargs["solve_cloudflare"] = True

                    if page_action is not None:
                        fetch_kwargs["page_action"] = page_action

                    page = await self._session.fetch(**fetch_kwargs)

                    # Mid-scrape CF detection: cookie may have expired
                    if (page and self.is_cf_challenge(page)
                            and self._cookies
                            and self.on_cf_detected_callback):
                        log.warning(
                            f"[fetch] CF challenge detected for {url} — "
                            "cookies may have expired, requesting refresh..."
                        )
                        refreshed = await self.on_cf_detected_callback(self)
                        if refreshed:
                            # Re-inject cookies and retry this fetch
                            self._cookies_injected = False
                            await self._ensure_session()
                            page = await self._session.fetch(**fetch_kwargs)
                            if page and not self.is_cf_challenge(page):
                                await self.delay.on_success()
                                return page

                    await self.delay.on_success()
                    return page
                except Exception as e:
                    log.warning(f"[fetch] Attempt {attempt + 1}/{retries} failed for {url}: {e}")
                    await self.delay.on_failure()
                    await asyncio.sleep(1.5 * (attempt + 1) + random.uniform(0, 1))

            self.error_count += 1
            log.error(f"[fetch] All {retries} attempts failed: {url}")
            return None


    # ─── L2: Business Detail Extraction ──────────────────────────────────

    async def scrape_business(self, url: str, category: str):
        """Scrape a single business detail page."""
        if self.limit and self.scraped_count >= self.limit:
            return

        page = await self.fetch(url)
        if not page:
            self.skip_count += 1
            return

        if self.job.is_kuwait:
            data = extract_data_kuwait(page, url)
        else:
            data = extract_data(page, url, self.job.phone_prefix, self.job.country_name)
        if not data.get("Name"):
            log.warning(f"[L2] No name found, skipping: {url}")
            self.skip_count += 1
            return

        # Save to SQLite
        await self.storage.insert_business(url, category, data)

        # Append to raw CSV immediately
        await self.storage.append_raw_csv(data, category, url)

        self.scraped_count += 1
        log.info(f"[L2] #{self.scraped_count} {data['Name']} — {url}")

        # Signal shutdown if limit reached
        if self.limit and self.scraped_count >= self.limit:
            if not self.shutdown_event.is_set():
                if not self.job.quiet:
                    console.print(
                        Panel.fit(
                            f"[bold green]OK Limit of {self.limit} reached[/bold green] -- "
                            f"draining in-flight requests...",
                            border_style="green",
                        )
                    )
                log.info(f"[limit] Reached {self.scraped_count}/{self.limit} — shutting down producers")
            self.shutdown_event.set()

    # ─── Pipeline ────────────────────────────────────────────────────────

    async def run_pipeline(self, *, fresh: bool = False, category_filter: str = None,
                           dry_run: bool = False, quiet: bool = False):
        """Execute the full L0 -> L1 -> L2 producer-consumer pipeline."""
        self.start_time = time.monotonic()

        # Handle fresh start
        if fresh:
            self.storage.checkpoint_clear()
            if not quiet:
                console.print(f"[yellow][{self.job.country_key}] Checkpoints cleared, starting from scratch[/yellow]")

        # L0: Discover categories
        log.info(f"[{self.job.country_key}] Discovering categories…")
        if not quiet:
            console.print(f"[bold blue][{self.job.country_key}] Discovering categories…[/bold blue]")
        categories = await discover_categories(self)
        if not categories:
            log.error(f"[{self.job.country_key}] No categories discovered. Exiting.")
            console.print(f"[red][{self.job.country_key}] No categories discovered. Exiting.[/red]")
            return

        # Notify callback with discovered categories
        if self.categories_discovered_callback:
            self.categories_discovered_callback(categories)

        # Filter by category if specified
        if category_filter:
            targets = {c.strip().lower() for c in category_filter.split(",")}
            categories = [c for c in categories if c["slug"].lower() in targets]
            if not categories:
                log.error(f"[{self.job.country_key}] Category filter '{category_filter}' matched nothing")
                console.print(f"[red][{self.job.country_key}] Category filter '{category_filter}' matched nothing[/red]")
                return

        log.info(f"[{self.job.country_key}] {len(categories)} categories ready")
        if not quiet:
            console.print(f"[green][{self.job.country_key}] {len(categories)} categories ready[/green]")

        # Show checkpoint resume info
        cp_summary = self.storage.checkpoint_summary()
        if not quiet and (cp_summary["completed"] or cp_summary["in_progress"]):
            console.print(
                f"[cyan]>> Checkpoint: {cp_summary['completed']} categories complete, "
                f"{cp_summary['in_progress']} in-progress — resuming[/cyan]"
            )

        if dry_run:
            console.print(f"[yellow][{self.job.country_key}] [dry-run] Category discovery complete. Exiting.[/yellow]")
            return

        # ── Producer-consumer with Rich progress ─────────────────────────
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.concurrency * 10)
        cats_completed = 0
        total_cats = len(categories)
        producer_tasks: List[asyncio.Task] = []
        consumer_tasks: List[asyncio.Task] = []

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
        country_label = self.job.country_key.upper()
        self.cat_task_id = progress.add_task(f"[bold]{country_label} Categories[/bold]", total=total_cats)
        self.biz_task_id = progress.add_task(f"[bold]{country_label} Businesses[/bold]", total=0)

        try:
            # L1 producer
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
                        await crawl_category_listings(
                            self, cat["url"], cat["slug"], queue=queue
                        )
                    except asyncio.CancelledError:
                        return
                    finally:
                        cats_completed += 1
                        progress.update(self.cat_task_id, completed=cats_completed)

            # L2 consumer
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

            # Start consumers first
            consumer_tasks = [
                asyncio.create_task(l2_consumer())
                for _ in range(self.concurrency)
            ]
            # Start producers
            producer_tasks = [asyncio.create_task(l1_producer(cat)) for cat in categories]

            # Wait for all producers to finish
            await asyncio.gather(*producer_tasks, return_exceptions=True)

            if self.shutdown_event.is_set():
                while not queue.empty():
                    try:
                        queue.get_nowait()
                        queue.task_done()
                    except asyncio.QueueEmpty:
                        break
            else:
                for _ in consumer_tasks:
                    await queue.put(_SENTINEL)

            await asyncio.gather(*consumer_tasks, return_exceptions=True)

        except (asyncio.CancelledError, KeyboardInterrupt):
            console.print(
                f"\n[yellow][{self.job.country_key}] Ctrl+C — finishing in-flight tasks…[/yellow]"
            )
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
            all_tasks = producer_tasks + consumer_tasks
            if all_tasks:
                await asyncio.gather(*all_tasks, return_exceptions=True)
        finally:
            await self.storage.flush_csv()
            progress.stop()
            self.progress = None
            await self._close_session()

        elapsed = time.monotonic() - self.start_time
        console.print(
            f"[bold green][{self.job.country_key}] Pipeline complete — "
            f"{self.scraped_count} businesses in {elapsed:.0f}s[/bold green]"
        )

    # ─── Summary Report ──────────────────────────────────────────────────

    def print_summary(self):
        """Print a Rich summary table."""
        elapsed = time.monotonic() - self.start_time if self.start_time else 0
        rate = (self.scraped_count / (elapsed / 60)) if elapsed > 60 else self.scraped_count

        country_label = self.job.country_key.upper()
        table = Table(title=f"{country_label} Run Summary", show_header=False, border_style="blue")
        table.add_column("Metric", style="bold")
        table.add_column("Value", style="green")

        table.add_row("Duration", f"{elapsed:.0f}s ({elapsed / 60:.1f}m)")
        table.add_row("Businesses scraped", str(self.scraped_count))
        table.add_row("Skipped / errors", f"{self.skip_count} / {self.error_count}")
        table.add_row("Speed", f"{rate:.1f} / min")
        table.add_row("Concurrency", str(self.concurrency))
        table.add_row("Adaptive delay", f"{self.delay.current:.2f}s")

        total_db = self.storage.get_total_businesses()
        total_cats = self.storage.get_total_categories()
        table.add_row("DB records (total)", str(total_db))
        table.add_row("Categories in DB", str(total_cats))

        if os.path.exists(self.job.raw_csv_path):
            table.add_row("Raw CSV", self.job.raw_csv_path)

        console.print()
        console.print(table)
