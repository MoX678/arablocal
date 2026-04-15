#!/usr/bin/env python3
"""
Proxy Tester — Validates proxies against Cloudflare-protected URLs.

Tests each proxy from a file against a target URL (default: arablocal.com)
and reports which proxies pass CF checks and which fail.

Usage:
    python proxy_tester.py                          # Test all proxies in proxies.txt
    python proxy_tester.py -f my_proxies.txt        # Custom proxy file
    python proxy_tester.py --url https://uae.arablocal.com/business  # Custom target
    python proxy_tester.py --timeout 20             # Custom timeout (seconds)
    python proxy_tester.py --save working.txt       # Save working proxies to file
"""

import argparse
import asyncio
import sys
import time
import re
from pathlib import Path

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from scrapling.fetchers import AsyncStealthySession
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

console = Console()

# Default CF-protected test URLs
TEST_URLS = {
    "uae": "https://uae.arablocal.com/business",
    "sa": "https://arablocal.com/business",
    "kw": "https://kuwaitlocal.com/business/categories",
    "bh": "https://bahrain.arablocal.com/business",
    "qa": "https://qatar.arablocal.com/business",
    "om": "https://oman.arablocal.com/business",
}


def load_proxies(path: str) -> list[str]:
    """Load proxies from a file, one per line."""
    proxies = []
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if not line.startswith("http"):
                        line = f"http://{line}"
                    proxies.append(line)
    except FileNotFoundError:
        console.print(f"[red]File not found: {path}[/red]")
        sys.exit(1)
    return proxies


def is_cf_blocked(page) -> bool:
    """Check if the response is a Cloudflare challenge page."""
    status = getattr(page, "status", 0)
    if status >= 400:
        return True
    try:
        text_nodes = page.css("body *::text").getall()
        body_text = " ".join(t.strip() for t in text_nodes if t.strip())
        if len(body_text) < 200 and "Just a moment" in body_text:
            return True
        if len(body_text) < 200 and "Cloudflare" in body_text:
            return True
    except Exception:
        pass
    return False


def extract_page_title(page) -> str:
    """Extract the page title for result reporting."""
    try:
        title = page.css("title::text").get("").strip()
        return title[:60] if title else "(no title)"
    except Exception:
        return "(error)"


async def test_single_proxy(proxy: str, url: str, timeout_ms: int) -> dict:
    """Test a single proxy against the target URL."""
    result = {
        "proxy": proxy,
        "status": "FAIL",
        "http_code": 0,
        "cf_blocked": False,
        "latency_ms": 0,
        "title": "",
        "error": "",
    }

    session = None
    start = time.monotonic()
    try:
        session = AsyncStealthySession(
            headless=True,
            disable_resources=True,
            block_ads=True,
            network_idle=True,
            timeout=timeout_ms,
            proxy=proxy,
        )
        await session.start()

        page = await session.fetch(
            url=url,
            solve_cloudflare=True,
        )

        elapsed = (time.monotonic() - start) * 1000
        result["latency_ms"] = int(elapsed)

        if page is None:
            result["error"] = "No response"
            return result

        result["http_code"] = getattr(page, "status", 0)

        if is_cf_blocked(page):
            result["cf_blocked"] = True
            result["status"] = "CF_BLOCKED"
            result["error"] = "Cloudflare challenge not bypassed"
        else:
            result["status"] = "OK"
            result["title"] = extract_page_title(page)

    except Exception as e:
        elapsed = (time.monotonic() - start) * 1000
        result["latency_ms"] = int(elapsed)
        err_msg = str(e)
        # Truncate long error messages
        if len(err_msg) > 80:
            err_msg = err_msg[:77] + "..."
        result["error"] = err_msg
    finally:
        if session:
            try:
                await session.close()
            except Exception:
                pass

    return result


async def test_no_proxy(url: str, timeout_ms: int) -> dict:
    """Test direct connection (no proxy) as baseline."""
    result = {
        "proxy": "(direct)",
        "status": "FAIL",
        "http_code": 0,
        "cf_blocked": False,
        "latency_ms": 0,
        "title": "",
        "error": "",
    }

    session = None
    start = time.monotonic()
    try:
        session = AsyncStealthySession(
            headless=True,
            disable_resources=True,
            block_ads=True,
            network_idle=True,
            timeout=timeout_ms,
        )
        await session.start()

        page = await session.fetch(url=url, solve_cloudflare=True)

        elapsed = (time.monotonic() - start) * 1000
        result["latency_ms"] = int(elapsed)

        if page is None:
            result["error"] = "No response"
            return result

        result["http_code"] = getattr(page, "status", 0)

        if is_cf_blocked(page):
            result["cf_blocked"] = True
            result["status"] = "CF_BLOCKED"
            result["error"] = "Cloudflare challenge not bypassed"
        else:
            result["status"] = "OK"
            result["title"] = extract_page_title(page)

    except Exception as e:
        elapsed = (time.monotonic() - start) * 1000
        result["latency_ms"] = int(elapsed)
        err_msg = str(e)
        if len(err_msg) > 80:
            err_msg = err_msg[:77] + "..."
        result["error"] = err_msg
    finally:
        if session:
            try:
                await session.close()
            except Exception:
                pass

    return result


async def run_tests(proxies: list[str], url: str, timeout_ms: int, include_direct: bool):
    """Run proxy tests sequentially (each needs its own browser context)."""
    results = []

    total = len(proxies) + (1 if include_direct else 0)

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("[cyan]Testing proxies...", total=total)

        # Test direct connection first as baseline
        if include_direct:
            progress.update(task, description="[cyan]Testing direct connection...")
            direct_result = await test_no_proxy(url, timeout_ms)
            results.append(direct_result)
            status_icon = "[green]OK[/green]" if direct_result["status"] == "OK" else "[red]FAIL[/red]"
            console.print(f"  (direct) → {status_icon} ({direct_result['latency_ms']}ms)")
            progress.advance(task)

        # Test each proxy
        for i, proxy in enumerate(proxies, 1):
            # Mask credentials in display
            display_proxy = re.sub(r"://([^:]+):([^@]+)@", r"://***:***@", proxy)
            progress.update(task, description=f"[cyan]Testing {display_proxy}...")

            result = await test_single_proxy(proxy, url, timeout_ms)
            results.append(result)

            if result["status"] == "OK":
                console.print(f"  {display_proxy} → [green]OK[/green] ({result['latency_ms']}ms)")
            elif result["status"] == "CF_BLOCKED":
                console.print(f"  {display_proxy} → [yellow]CF_BLOCKED[/yellow] ({result['latency_ms']}ms)")
            else:
                console.print(f"  {display_proxy} → [red]FAIL[/red] {result['error'][:50]}")

            progress.advance(task)

    return results


def print_results(results: list[dict]):
    """Print a summary table of all test results."""
    table = Table(title="Proxy Test Results", show_lines=True)
    table.add_column("Proxy", style="cyan", max_width=40)
    table.add_column("Status", justify="center")
    table.add_column("HTTP", justify="center")
    table.add_column("Latency", justify="right")
    table.add_column("CF", justify="center")
    table.add_column("Title / Error", max_width=50)

    for r in results:
        # Mask credentials
        proxy_display = re.sub(r"://([^:]+):([^@]+)@", r"://***:***@", r["proxy"])

        if r["status"] == "OK":
            status = "[green]OK[/green]"
        elif r["status"] == "CF_BLOCKED":
            status = "[yellow]BLOCKED[/yellow]"
        else:
            status = "[red]FAIL[/red]"

        http = str(r["http_code"]) if r["http_code"] else "-"
        latency = f"{r['latency_ms']}ms"
        cf = "[yellow]YES[/yellow]" if r["cf_blocked"] else "[green]NO[/green]"
        detail = r["title"] if r["status"] == "OK" else r["error"][:50]

        table.add_row(proxy_display, status, http, latency, cf, detail)

    console.print()
    console.print(table)

    # Summary counts
    ok = sum(1 for r in results if r["status"] == "OK")
    blocked = sum(1 for r in results if r["status"] == "CF_BLOCKED")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    console.print(f"\n[bold]Summary:[/bold] [green]{ok} OK[/green] | "
                  f"[yellow]{blocked} CF-blocked[/yellow] | [red]{failed} failed[/red] "
                  f"out of {len(results)} tested")


def save_working(results: list[dict], path: str):
    """Save working proxies to a file."""
    working = [r["proxy"] for r in results if r["status"] == "OK" and r["proxy"] != "(direct)"]
    if not working:
        console.print("[yellow]No working proxies to save.[/yellow]")
        return
    with open(path, "w") as f:
        for p in working:
            f.write(p + "\n")
    console.print(f"[green]Saved {len(working)} working proxies to {path}[/green]")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Test proxies against Cloudflare-protected URLs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-f", "--file", default="proxies.txt",
        help="Path to proxy list file (default: proxies.txt)",
    )
    parser.add_argument(
        "--url", default=None,
        help="Target URL to test against (default: uae.arablocal.com/business)",
    )
    parser.add_argument(
        "--country", default="uae", choices=TEST_URLS.keys(),
        help="Use preset test URL for this country (default: uae)",
    )
    parser.add_argument(
        "--timeout", type=int, default=20,
        help="Timeout per proxy test in seconds (default: 20)",
    )
    parser.add_argument(
        "--save", default=None,
        help="Save working proxies to this file",
    )
    parser.add_argument(
        "--no-direct", action="store_true",
        help="Skip direct connection baseline test",
    )
    return parser.parse_args()


async def async_main():
    args = parse_args()

    # Determine target URL
    url = args.url or TEST_URLS.get(args.country, TEST_URLS["uae"])
    timeout_ms = args.timeout * 1000

    # Load proxies
    proxies = load_proxies(args.file)
    if not proxies:
        console.print("[red]No proxies found in file.[/red]")
        return

    console.print(f"[bold blue]Proxy Tester[/bold blue]")
    console.print(f"  Target: {url}")
    console.print(f"  Proxies: {len(proxies)} from {args.file}")
    console.print(f"  Timeout: {args.timeout}s per test")
    console.print()

    # Run tests
    results = await run_tests(
        proxies, url, timeout_ms,
        include_direct=not args.no_direct,
    )

    # Print results
    print_results(results)

    # Save working proxies
    if args.save:
        save_working(results, args.save)


def main():
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        console.print("\n[yellow]Test interrupted.[/yellow]")


if __name__ == "__main__":
    main()
