"""Qt worker threads bridging asyncio scraper engine with the GUI.

Workers:
  - CategoryFetchWorker: discover categories for a country
  - ProxyTestWorker: concurrent proxy testing with HTTP-first + browser fallback
  - ScrapeWorker: parallel multi-country pipeline with granular signals
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from PyQt6.QtCore import QThread, pyqtSignal

from core.config import BASE_URLS, COUNTRY_INFO, JobConfig, resolve_concurrency
from core.discovery import discover_categories
from core.engine import ArabLocalEngine

log = logging.getLogger("arablocal")


# ─── Qt-compatible logging handler ──────────────────────────────────────────

class QtLogHandler(logging.Handler):
    """Bridges Python logging → a callable (typically a Qt signal emit).

    Usage:
        handler = QtLogHandler(callback)
        logging.getLogger("arablocal").addHandler(handler)
    """

    _LEVEL_MAP = {
        logging.DEBUG: "INFO",
        logging.INFO: "INFO",
        logging.WARNING: "WARNING",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "ERROR",
    }

    def __init__(self, callback):
        super().__init__()
        self._callback = callback

    def emit(self, record: logging.LogRecord):
        try:
            level = self._LEVEL_MAP.get(record.levelno, "INFO")
            msg = self.format(record)
            # Extract country key from message like "[L0]", "[L2]", "[session]"
            self._callback("CORE", level, msg)
        except Exception:
            self.handleError(record)


# Test URLs for proxy validation — 5 arablocal endpoints
PROXY_TEST_URLS = [
    "https://uae.arablocal.com",
    "https://arablocal.com",
    "https://qatar.arablocal.com",
    "https://oman.arablocal.com",
    "https://kuwaitlocal.com",
]


@dataclass
class ScrapeStats:
    scraped: int = 0
    skipped: int = 0
    errors: int = 0
    elapsed: float = 0.0
    rate: float = 0.0
    current_category: str = ""
    categories_done: int = 0
    categories_total: int = 0


# ─── Category fetch worker ──────────────────────────────────────────────────

class CategoryFetchWorker(QThread):
    """Fetch categories for one or more countries in the background."""

    categories_ready = pyqtSignal(str, list)       # country_key, [{name, slug, url}, ...]
    error = pyqtSignal(str, str)                    # country_key, error_message
    finished_signal = pyqtSignal(str)               # country_key

    def __init__(self, country: str, parent=None):
        super().__init__(parent)
        self.country = country.lower()

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            cats = loop.run_until_complete(self._fetch())
            self.categories_ready.emit(self.country, cats)
        except Exception as e:
            self.error.emit(self.country, str(e))
        finally:
            loop.close()
            self.finished_signal.emit(self.country)

    async def _fetch(self):
        job = JobConfig(
            country=self.country,
            concurrency=2,
            quiet=True,
            output_dir=os.path.join("output", self.country),
        )
        job.ensure_dirs()
        engine = ArabLocalEngine(job=job, concurrency=2)
        try:
            cats = await discover_categories(engine)
            return cats or []
        finally:
            await engine._close_session()


# ─── Multi-country category fetch worker ─────────────────────────────────────

class MultiCategoryFetchWorker(QThread):
    """Fetch categories for multiple countries in parallel."""

    categories_ready = pyqtSignal(str, list)
    country_error = pyqtSignal(str, str)
    all_finished = pyqtSignal()

    def __init__(self, countries: List[str], parent=None):
        super().__init__(parent)
        self.countries = countries

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._fetch_all())
        finally:
            loop.close()
            self.all_finished.emit()

    async def _fetch_all(self):
        # Limit concurrent browser sessions to avoid spawning too many windows
        sem = asyncio.Semaphore(2)
        async def limited(c):
            async with sem:
                return await self._fetch_one(c)
        tasks = [limited(c) for c in self.countries]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_one(self, country: str):
        try:
            job = JobConfig(
                country=country,
                concurrency=2,
                quiet=True,
                output_dir=os.path.join("output", country),
            )
            job.ensure_dirs()
            engine = ArabLocalEngine(job=job, concurrency=2)
            try:
                cats = await discover_categories(engine)
                self.categories_ready.emit(country, cats or [])
            finally:
                await engine._close_session()
        except Exception as e:
            self.country_error.emit(country, str(e))


# ─── Proxy test worker — concurrent HTTP-first ──────────────────────────────

class ProxyTestWorker(QThread):
    """Test proxies concurrently using HTTP HEAD requests.

    Much faster than browser-based testing. Tests against 5 arablocal URLs.
    """

    proxy_result = pyqtSignal(str, bool, float)    # proxy, alive, latency_ms
    progress = pyqtSignal(int, int)                 # current, total
    finished_signal = pyqtSignal(int, int)          # alive_count, total

    def __init__(self, proxies: List[str], concurrency: int = 5, parent=None):
        super().__init__(parent)
        self.proxies = proxies
        self.concurrency = concurrency

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._test_all())
        finally:
            loop.close()

    async def _test_all(self):
        alive = 0
        total = len(self.proxies)
        sem = asyncio.Semaphore(self.concurrency)
        completed = 0

        async def test_proxy(proxy):
            nonlocal alive, completed
            async with sem:
                ok, latency = await self._test_one_http(proxy)
                if ok:
                    alive += 1
                completed += 1
                self.proxy_result.emit(proxy, ok, latency)
                self.progress.emit(completed, total)

        tasks = [asyncio.create_task(test_proxy(p)) for p in self.proxies]
        await asyncio.gather(*tasks, return_exceptions=True)
        self.finished_signal.emit(alive, total)

    async def _test_one_http(self, proxy: str) -> tuple[bool, float]:
        """Test proxy with HTTP request to uae.arablocal.com."""
        loop = asyncio.get_event_loop()
        test_url = PROXY_TEST_URLS[0]  # UAE as primary

        start = time.monotonic()
        try:
            proxy_handler = urllib.request.ProxyHandler({
                "http": proxy,
                "https": proxy,
            })
            opener = urllib.request.build_opener(proxy_handler)
            req = urllib.request.Request(
                test_url,
                method="GET",
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            )
            result = await loop.run_in_executor(
                None,
                lambda: opener.open(req, timeout=10),
            )
            elapsed = (time.monotonic() - start) * 1000
            status = result.getcode()
            return status == 200, elapsed
        except Exception:
            elapsed = (time.monotonic() - start) * 1000
            return False, elapsed


# ─── Main scrape worker — parallel multi-country ────────────────────────────

class ScrapeWorker(QThread):
    """Run scrape pipelines for multiple countries in parallel.

    Emits granular signals for real-time GUI updates.
    """

    # Signals
    category_discovered = pyqtSignal(str, int)       # country_key, count
    category_started = pyqtSignal(str, str)           # country_key, category_slug
    category_progress = pyqtSignal(str, str, int, int, int)  # country, cat_name, page, total_pages, urls
    business_scraped = pyqtSignal(str, str, str)      # country_key, name, url
    stats_update = pyqtSignal(str, object)            # country_key, ScrapeStats
    log_message = pyqtSignal(str, str, str)           # country_key, level, message
    job_started = pyqtSignal(str)                      # country_key
    job_completed = pyqtSignal(str, object)            # country_key, ScrapeStats
    job_error = pyqtSignal(str, str)                   # country_key, error_msg
    checkpoint_info = pyqtSignal(str, int, int)        # country_key, completed_cats, total_cats
    pipeline_finished = pyqtSignal()

    def __init__(self, jobs: List[JobConfig], proxies: List[str] = None, parent=None):
        super().__init__(parent)
        self.jobs = jobs
        self.proxies = proxies or []
        self._cancel_requested = False
        self._engines: Dict[str, ArabLocalEngine] = {}

    def request_cancel(self):
        self._cancel_requested = True
        for engine in self._engines.values():
            if hasattr(engine, 'shutdown_event'):
                engine.shutdown_event.set()

    def cancel_country(self, country: str):
        """Cancel a single country's pipeline."""
        engine = self._engines.get(country)
        if engine and hasattr(engine, 'shutdown_event'):
            engine.shutdown_event.set()

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._run_all())
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            print(f"[CRASH] Pipeline crashed: {tb}", flush=True)
            self.log_message.emit("ALL", "ERROR", f"Pipeline crashed: {e}")
        finally:
            loop.close()
            self.pipeline_finished.emit()

    async def _run_all(self):
        """Run all country jobs in parallel."""
        if len(self.jobs) == 1:
            await self._run_single(self.jobs[0])
        else:
            tasks = [
                asyncio.create_task(self._run_single(job))
                for job in self.jobs
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_single(self, job: JobConfig):
        """Run one country's pipeline with signal hooks."""
        key = job.country_key

        if self._cancel_requested:
            return

        self.job_started.emit(key)
        self.log_message.emit(key, "INFO", f"Starting {key.upper()} pipeline...")

        engine = ArabLocalEngine(job=job, concurrency=job.concurrency, proxies=self.proxies)
        self._engines[key] = engine

        # Hook category progress callback
        def _cat_progress_cb(cat_name, page_num, total_pages, new_urls, _key=key):
            self.category_progress.emit(_key, cat_name, page_num, total_pages, new_urls)
        engine.category_progress_callback = _cat_progress_cb

        # Hook into scrape_business to emit signals
        orig_scrape = engine.scrape_business

        async def hooked_scrape(url, category, _engine=engine, _orig=orig_scrape):
            await _orig(url, category)
            # Extract name from storage if available
            name = ""
            try:
                import sqlite3, json
                conn = sqlite3.connect(_engine.job.db_path, timeout=5)
                row = conn.execute(
                    "SELECT data FROM businesses WHERE url = ?", (url,)
                ).fetchone()
                if row:
                    d = json.loads(row[0])
                    name = d.get("Name", "")
                conn.close()
            except Exception:
                pass

            self.business_scraped.emit(key, name or url.split("/")[-1][:40], url)

            elapsed = time.monotonic() - _engine.start_time if _engine.start_time else 0
            rate = (_engine.scraped_count / (elapsed / 60)) if elapsed > 60 else _engine.scraped_count
            stats = ScrapeStats(
                scraped=_engine.scraped_count,
                skipped=_engine.skip_count,
                errors=_engine.error_count,
                elapsed=elapsed,
                rate=rate,
            )
            self.stats_update.emit(key, stats)

        engine.scrape_business = hooked_scrape

        try:
            # Emit checkpoint resume info
            cp = engine.storage.checkpoint_summary()
            if cp["completed"] or cp["in_progress"]:
                self.checkpoint_info.emit(key, cp["completed"], cp["completed"] + cp["in_progress"])
                self.log_message.emit(
                    key, "INFO",
                    f"Resuming: {cp['completed']} categories complete, "
                    f"{cp['in_progress']} in-progress"
                )

            cat_filter = ",".join(job.categories) if job.categories else None
            await engine.run_pipeline(
                fresh=job.fresh,
                category_filter=cat_filter,
                dry_run=job.dry_run,
                quiet=True,
            )

            elapsed = time.monotonic() - engine.start_time if engine.start_time else 0
            rate = (engine.scraped_count / (elapsed / 60)) if elapsed > 60 else engine.scraped_count
            final_stats = ScrapeStats(
                scraped=engine.scraped_count,
                skipped=engine.skip_count,
                errors=engine.error_count,
                elapsed=elapsed,
                rate=rate,
            )
            self.job_completed.emit(key, final_stats)
            self.log_message.emit(
                key, "SUCCESS",
                f"Completed: {engine.scraped_count} businesses in {elapsed:.0f}s"
            )

            # Export sorted CSVs
            engine.storage.export_sorted_csvs()

        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            print(f"[CRASH] {key}: {tb}", flush=True)
            self.job_error.emit(key, str(e))
            self.log_message.emit(key, "ERROR", str(e))
        finally:
            self._engines.pop(key, None)
            await engine._close_session()
