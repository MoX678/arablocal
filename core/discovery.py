"""Category discovery and listing pagination."""

from __future__ import annotations

import asyncio
import logging
import random
import re
import urllib.request
from typing import TYPE_CHECKING, List, Optional
from urllib.parse import urljoin, urlparse

from rich.console import Console

if TYPE_CHECKING:
    from core.engine import ArabLocalEngine

log = logging.getLogger("arablocal")
console = Console(quiet=True)


# ─── Direct fetch helper (no proxy) for discovery fallback ───────────────────

async def _direct_fetch(url: str):
    """Fetch a page directly (no proxy) using a throwaway stealth session."""
    from scrapling.fetchers import AsyncStealthySession

    session = AsyncStealthySession(
        headless=True,
        disable_resources=True,
        block_ads=True,
        timeout=15000,
    )
    try:
        await session.start()
        page = await session.fetch(url=url)
        return page
    except Exception as e:
        log.warning(f"[L0] Direct fallback fetch failed: {e}")
        return None
    finally:
        try:
            await session.close()
        except Exception:
            pass


# ─── L0: Category Discovery ─────────────────────────────────────────────────

async def discover_categories(engine: "ArabLocalEngine") -> List[dict]:
    """Auto-discover all categories from the business directory.

    For Kuwait, delegates to discover_categories_kuwait().
    On proxy failure, retries with direct connection.
    """
    if engine.job.is_kuwait:
        return await discover_categories_kuwait(engine)

    url = f"{engine.job.base_url}/business"
    log.info(f"[L0] Discovering categories from {url}")

    page = await engine.fetch(url)
    if not page and engine.proxy_pool:
        log.warning("[L0] Fetch failed with proxies — retrying direct")
        page = await _direct_fetch(url)
    if not page:
        log.error("[L0] Could not load business directory page")
        return []

    categories = []
    seen_slugs: set = set()

    all_hrefs = page.css("a[href*='/business/category/']::attr(href)").getall()
    log.info(f"[L0] Found {len(all_hrefs)} raw category hrefs")

    # Proxy returned a page but no categories (likely captcha/block) — retry direct
    if not all_hrefs and engine.proxy_pool:
        log.warning("[L0] Page loaded via proxy but 0 categories found — retrying direct")
        page2 = await _direct_fetch(url)
        if page2:
            all_hrefs = page2.css("a[href*='/business/category/']::attr(href)").getall()
            log.info(f"[L0] Direct retry found {len(all_hrefs)} category hrefs")

    for href in all_hrefs:
        if not href:
            continue
        full_url = urljoin(engine.job.base_url, href)
        path = urlparse(full_url).path
        match = re.search(r"/business/category/([^/?#]+)", path)
        if not match:
            continue
        slug = match.group(1)
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        name = slug.replace("-", " ").replace("%2C", ",").replace("%26", "&").title()
        cat = {"slug": slug, "name": name, "url": full_url}
        categories.append(cat)
        await engine.storage.insert_category(slug, name, full_url)

    log.info(f"[L0] Found {len(categories)} categories")
    for c in categories:
        log.info(f"  • {c['name']} -> {c['url']}")

    return categories


async def discover_categories_kuwait(engine: "ArabLocalEngine") -> List[dict]:
    """Kuwait: categories at /business/categories with /business/categories/SLUG links."""
    url = f"{engine.job.base_url}/business/categories"
    log.info(f"[L0-KW] Discovering categories from {url}")

    page = await engine.fetch(url)
    if not page and engine.proxy_pool:
        log.warning("[L0-KW] Fetch failed with proxies — retrying direct")
        page = await _direct_fetch(url)
    if not page:
        log.error("[L0-KW] Could not load categories page")
        return []

    categories = []
    seen_slugs: set = set()

    all_hrefs = page.css("a[href*='/business/categories/']::attr(href)").getall()
    log.info(f"[L0-KW] Found {len(all_hrefs)} raw category hrefs")

    # Proxy returned a page but no categories — retry direct
    if not all_hrefs and engine.proxy_pool:
        log.warning("[L0-KW] Page loaded via proxy but 0 categories found — retrying direct")
        page2 = await _direct_fetch(url)
        if page2:
            all_hrefs = page2.css("a[href*='/business/categories/']::attr(href)").getall()
            log.info(f"[L0-KW] Direct retry found {len(all_hrefs)} category hrefs")

    for href in all_hrefs:
        if not href:
            continue
        full_url = urljoin(engine.job.base_url, href)
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
        await engine.storage.insert_category(slug, name, full_url)

    log.info(f"[L0-KW] Found {len(categories)} categories")
    for c in categories:
        log.info(f"  . {c['name']} -> {c['url']}")

    return categories


# ─── L1: Pagination — Collect business URLs ─────────────────────────────────

async def crawl_category_listings(engine: "ArabLocalEngine", cat_url: str,
                                   cat_name: str,
                                   queue: Optional[asyncio.Queue] = None,
                                   depth: int = 0) -> int:
    """Paginate through a category and push business URLs into queue.

    Returns the count of new URLs found.
    If the page is a parent category (no business links, only sub-category links),
    auto-expands into sub-categories (max 1 level deep).
    """
    if engine.job.is_kuwait:
        return await kw_crawl_listings(engine, cat_url, cat_name, queue)

    # Check checkpoint for resume
    checkpoint = engine.storage.checkpoint_get(cat_name)
    if checkpoint and checkpoint["completed"]:
        log.info(f"[L1] {cat_name} — already completed (checkpoint), skipping")
        return 0

    url_count = 0
    existing = engine.storage.get_existing_urls(cat_name)
    page_num = 1
    current_url = cat_url

    # Resume from checkpoint if available
    if checkpoint and checkpoint["last_page"] > 1:
        page_num = checkpoint["last_page"] + 1
        url_count = checkpoint["urls_found"]
        current_url = f"{cat_url}/page:{page_num}"
        if not engine.job.quiet:
            console.print(f"  [yellow]>> Resuming {cat_name} from page {page_num}[/yellow]")
        log.info(f"[L1] Resuming {cat_name} from page {page_num} ({url_count} URLs already found)")

    first_page = True
    total_pages_est = 0
    while current_url:
        if engine.shutdown_event.is_set():
            break
        if engine.limit and engine.scraped_count >= engine.limit:
            log.info(f"[L1] Global limit reached, stopping {cat_name}")
            break

        if not engine.job.quiet:
            console.print(f"  [dim]{cat_name}[/dim] page {page_num}…", highlight=False)
        log.info(f"[L1] {cat_name} page {page_num}: {current_url}")
        page = await engine.fetch(current_url)
        if not page:
            break

        # Extract business detail links
        links = page.css('a[href*="/business/view/"]::attr(href)').getall()

        # Detect total pages from pagination on first page
        if first_page and total_pages_est == 0:
            all_page_hrefs = page.css('.pagination a::attr(href)').getall()
            max_pg = 1
            for ph in all_page_hrefs:
                pm = re.search(r'/page:(\d+)', ph)
                if pm:
                    max_pg = max(max_pg, int(pm.group(1)))
            if max_pg > 1:
                total_pages_est = max_pg
            else:
                # Fallback: if there's a next link, estimate at least 2
                if page.css('a[rel="next"]::attr(href)').get(""):
                    total_pages_est = 0  # unknown, keep discovering

        # Detect parent category: no business links but has sub-category links
        if first_page and len(links) == 0 and depth < 1:
            sub_cat_hrefs = page.css('a[href*="/business/category/"]::attr(href)').getall()
            sub_cats = []
            seen_sub: set = set()
            for href in sub_cat_hrefs:
                if not href:
                    continue
                full = urljoin(engine.job.base_url, href)
                sub_match = re.search(r"/business/category/([^/?#]+)", urlparse(full).path)
                if not sub_match:
                    continue
                sub_slug = sub_match.group(1)
                if sub_slug in seen_sub or sub_slug == cat_name.lower().replace(" ", "-"):
                    continue
                seen_sub.add(sub_slug)
                sub_cats.append((sub_slug, full))

            if sub_cats:
                sub_name = cat_name
                if not engine.job.quiet:
                    console.print(
                        f"  [yellow]{cat_name} is a parent category "
                        f"— expanding {len(sub_cats)} sub-categories…[/yellow]"
                    )
                log.info(f"[L1] {cat_name} is parent category with {len(sub_cats)} sub-categories")
                total = 0
                for sub_slug, sub_url in sub_cats:
                    if engine.shutdown_event.is_set():
                        break
                    if engine.limit and engine.scraped_count >= engine.limit:
                        break
                    total += await crawl_category_listings(
                        engine, sub_url, sub_name, queue, depth=depth + 1
                    )
                return total

        first_page = False
        new_count = 0
        seen_on_page: set = set()
        for href in links:
            if not href:
                continue
            full = urljoin(engine.job.base_url, href)
            path = urlparse(full).path
            if "/business/view/" not in path or full in seen_on_page:
                continue
            seen_on_page.add(full)
            if full not in existing:
                new_count += 1
                url_count += 1
                if queue is not None:
                    while not engine.shutdown_event.is_set():
                        try:
                            await asyncio.wait_for(queue.put((full, cat_name)), timeout=2.0)
                            break
                        except asyncio.TimeoutError:
                            continue
                    if engine.shutdown_event.is_set():
                        break

        log.info(f"[L1] Found {len(seen_on_page)} links, {new_count} new "
                 f"(skipped {len(seen_on_page) - new_count} existing)")

        # Save checkpoint after each page
        await engine.storage.checkpoint_save(cat_name, page_num, url_count)

        # Emit category progress callback
        if engine.category_progress_callback:
            engine.category_progress_callback(cat_name, page_num, total_pages_est, new_count)

        # Update progress bar total as URLs are discovered
        if new_count and engine.progress and engine.biz_task_id is not None:
            engine.progress.update(
                engine.biz_task_id,
                total=(engine.progress.tasks[engine.biz_task_id].total or 0) + new_count,
            )

        if len(seen_on_page) == 0:
            break

        # Find next page
        next_link = (
            page.css('a[rel="next"]::attr(href)').get("")
            or page.css('.pagination .next a::attr(href)').get("")
        )
        if next_link:
            current_url = urljoin(engine.job.base_url, next_link)
        else:
            page_num += 1
            current_url = f"{cat_url}/page:{page_num}"

    # Mark completed if we didn't exit due to shutdown/limit
    if not engine.shutdown_event.is_set() and not (engine.limit and engine.scraped_count >= engine.limit):
        await engine.storage.checkpoint_save(cat_name, page_num, url_count, completed=True)

    log.info(f"[L1] Total URLs queued for {cat_name}: {url_count}")
    return url_count


# ─── Kuwait L1: AJAX "Load more" pagination ─────────────────────────────────

async def kw_crawl_listings(engine: "ArabLocalEngine", cat_url: str,
                             cat_name: str,
                             queue: Optional[asyncio.Queue] = None) -> int:
    """Kuwait: AJAX 'Load more' pagination via direct HTTP requests."""
    checkpoint = engine.storage.checkpoint_get(cat_name)
    if checkpoint and checkpoint["completed"]:
        log.info(f"[L1-KW] {cat_name} — already completed (checkpoint), skipping")
        return 0

    existing = engine.storage.get_existing_urls(cat_name)

    if not engine.job.quiet:
        console.print(f"  [dim]{cat_name}[/dim] loading listings...", highlight=False)
    log.info(f"[L1-KW] {cat_name}: {cat_url}")

    # Step 1: Browser fetch to get initial page + data-total
    page = await engine.fetch(cat_url)
    if not page:
        return 0

    gridlist = page.css(".gridlist")
    total = int(gridlist[0].attrib.get("data-total", "0")) if gridlist else 0
    initial_count = len(page.css(".ggrid"))
    log.info(f"[L1-KW] {cat_name}: total={total}, initial={initial_count}")

    import math
    kw_total_pages = max(1, math.ceil(total / 20)) if total > 0 else 1

    # Extract links helper
    kw_skip = {"/categories", "/governorates", "/tags", "/recent"}

    def _extract_biz_links(hrefs):
        links = set()
        for href in hrefs:
            if not href:
                continue
            full = urljoin(engine.job.base_url, href)
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
            while not engine.shutdown_event.is_set():
                try:
                    await asyncio.wait_for(queue.put((full, cat_name)), timeout=2.0)
                    break
                except asyncio.TimeoutError:
                    continue
            if engine.shutdown_event.is_set():
                break

    log.info(f"[L1-KW] {cat_name}: page 1 -> {len(seen)} links, {url_count} new")

    # Emit progress callback for initial page
    if engine.category_progress_callback:
        engine.category_progress_callback(cat_name, 1, kw_total_pages, url_count)

    # Step 2: AJAX pages via urllib
    if total > initial_count and not engine.shutdown_event.is_set():
        offset = initial_count
        ajax_headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/html, */*; q=0.01",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": cat_url,
        }
        page_num = 2
        loop = asyncio.get_event_loop()

        while offset < total and not engine.shutdown_event.is_set():
            if engine.limit and engine.scraped_count >= engine.limit:
                break
            ajax_url = f"{cat_url}?loadmore=yes&offset={offset}&totalrecord={total}"
            try:
                await engine.delay.wait()
                req = urllib.request.Request(ajax_url, headers=ajax_headers)
                body = await loop.run_in_executor(
                    None,
                    lambda r=req: urllib.request.urlopen(r, timeout=15).read().decode("utf-8", "replace"),
                )
                if not body or body.strip() == "0" or "<!DOCTYPE" in body[:100]:
                    log.info(f"[L1-KW] {cat_name}: AJAX page {page_num} -> empty/end")
                    break

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
                        while not engine.shutdown_event.is_set():
                            try:
                                await asyncio.wait_for(queue.put((full, cat_name)), timeout=2.0)
                                break
                            except asyncio.TimeoutError:
                                continue
                        if engine.shutdown_event.is_set():
                            break

                if not engine.job.quiet:
                    console.print(f"  [dim]{cat_name}[/dim] page {page_num} +{batch_new}", highlight=False)
                log.info(f"[L1-KW] {cat_name}: AJAX page {page_num} -> {len(new_links)} links, {batch_new} new")
                await engine.delay.on_success()

                # Emit progress callback for AJAX page
                if engine.category_progress_callback:
                    engine.category_progress_callback(cat_name, page_num, kw_total_pages, batch_new)

                frag_count = len(frag.css(".ggrid"))
                offset += frag_count if frag_count else len(new_links)
                page_num += 1

            except Exception as e:
                log.warning(f"[L1-KW] AJAX page {page_num} failed: {e}")
                await engine.delay.on_failure()
                break

    # Update progress bar
    if url_count and engine.progress and engine.biz_task_id is not None:
        engine.progress.update(
            engine.biz_task_id,
            total=(engine.progress.tasks[engine.biz_task_id].total or 0) + url_count,
        )

    if not engine.shutdown_event.is_set():
        await engine.storage.checkpoint_save(cat_name, 1, url_count, completed=True)

    log.info(f"[L1-KW] {cat_name}: TOTAL {url_count} new URLs queued (from {len(seen)} found)")
    return url_count
