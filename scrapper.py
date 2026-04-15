#!/usr/bin/env python3
"""
ArabLocal Universal Scraper v3 — Modular, Multi-Country, CLI-Driven
===================================================================
Scrapes business listings from arablocal.com network with:
  - Per-country isolated output (DB, CSV, logs)
  - Multi-country parallel execution
  - SQLite staging (zero data loss on crash)
  - Stealth browser with proxy rotation
  - Priority-sorted CSV export

Usage:
    python scrapper.py --country sa --limit 5             # SA, 5 businesses
    python scrapper.py --job sa:5 --job uae:10            # Parallel: SA(5) + UAE(10)
    python scrapper.py --job sa:0:health-care,restaurants  # SA, specific categories
    python scrapper.py --country uae                      # All UAE categories
    python scrapper.py --export-only --country sa         # Re-export CSVs from DB
    python scrapper.py --dry-run --country kw             # Discover categories only
    python scrapper.py --threads auto --job sa --job uae  # Auto concurrency split
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import warnings
from typing import List

# Suppress harmless asyncio transport cleanup warnings on Windows
warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed transport")

# Force UTF-8 output on Windows to avoid cp1252 encoding errors with Rich
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from core.config import BASE_URLS, COUNTRY_INFO, JobConfig, parse_job_spec, resolve_concurrency
from core.engine import ArabLocalEngine

# ─── Logging & Console ──────────────────────────────────────────────────────
log = logging.getLogger("arablocal")
log.setLevel(logging.DEBUG)
console = Console()


# ─── Rich Help ───────────────────────────────────────────────────────────────

def print_rich_help():
    """Display a beautifully formatted help screen using Rich."""
    console.print()
    console.print(
        Panel.fit(
            "[bold white]ArabLocal Universal Scraper v3[/bold white]\n"
            "[dim]Modular · Multi-Country · Parallel · CLI-Driven[/dim]",
            border_style="blue",
        )
    )

    # ── Flags table ──
    flags = Table(
        title="[bold]Flags[/bold]",
        show_header=True,
        header_style="bold cyan",
        border_style="dim",
        pad_edge=False,
        show_lines=False,
    )
    flags.add_column("Flag", style="green", min_width=22, no_wrap=True)
    flags.add_column("Description", style="white")
    flags.add_column("Default", style="dim", justify="right")

    flags.add_row("--job, -j SPEC",      "Job spec: country[:limit[:cat1,cat2,...]]  (repeatable)", "")
    flags.add_row("--country CODE",       "Single country shortcut (use --job for multi)", "uae")
    flags.add_row("--category, -c SLUG",  "Filter to this category slug only", "all")
    flags.add_row("--limit, -l N",        "Max businesses to scrape per country (0 = ∞)", "0")
    flags.add_row("--threads, -t MODE",   "Concurrency: auto | max | <integer>", "auto")
    flags.add_row("--proxies, -p FILE",   "Proxy list file (one per line)", "none")
    flags.add_row("--output-dir, -o DIR", "Root output directory", "output/")
    flags.add_row("--combine",            "Merge all countries into combined_all.csv", "off")
    flags.add_row("--export-only, -e",    "Re-export CSVs from existing DB (no scrape)", "off")
    flags.add_row("--dry-run, -d",        "Discover categories only, no scraping", "off")
    flags.add_row("--fresh",              "Ignore checkpoints, scrape from scratch", "off")
    flags.add_row("--verbose, -v",        "Debug-level console output", "off")
    flags.add_row("--quiet, -q",          "Suppress progress bars, errors only", "off")
    console.print(flags)
    console.print()

    # ── Countries table ──
    countries = Table(
        title="[bold]Supported Countries[/bold]",
        show_header=True,
        header_style="bold cyan",
        border_style="dim",
    )
    countries.add_column("Code", style="green bold", justify="center")
    countries.add_column("Country", style="white")
    countries.add_column("Prefix", style="dim", justify="center")
    countries.add_column("Base URL", style="blue dim")
    for code in sorted(BASE_URLS):
        info = COUNTRY_INFO[code]
        countries.add_row(code.upper(), info["name"], info["code"], BASE_URLS[code])
    console.print(countries)
    console.print()

    # ── Examples ──
    examples = Table(
        title="[bold]Examples[/bold]",
        show_header=True,
        header_style="bold cyan",
        border_style="dim",
        pad_edge=False,
    )
    examples.add_column("Command", style="green", min_width=54, no_wrap=True)
    examples.add_column("What it does", style="white")
    examples.add_row("python scrapper.py --country sa --limit 5",            "Scrape 5 SA businesses")
    examples.add_row("python scrapper.py --job sa:5 --job uae:10",           "Parallel: SA(5) + UAE(10)")
    examples.add_row("python scrapper.py --job sa:0:health-care,restaurants", "SA, only 2 categories")
    examples.add_row("python scrapper.py --country kw --dry-run",            "Discover KW categories")
    examples.add_row("python scrapper.py --export-only --country sa",        "Re-export CSVs from DB")
    examples.add_row("python scrapper.py --threads 2 --job sa --job uae",    "2 threads split across jobs")
    examples.add_row("python scrapper.py --combine --job sa:5 --job uae:5",  "Parallel + combined CSV")
    console.print(examples)
    console.print()

    # ── Output structure ──
    console.print(
        Panel(
            "[dim]output/[/dim]\n"
            " ├── [bold]sa/[/bold]\n"
            " │   ├── sa_staging.db      [dim]SQLite staging database[/dim]\n"
            " │   ├── sa_raw.csv         [dim]Append-only raw output[/dim]\n"
            " │   ├── sa_all.csv         [dim]Sorted combined export[/dim]\n"
            " │   ├── categories/        [dim]Per-category CSVs[/dim]\n"
            " │   └── logs/              [dim]Timestamped log files[/dim]\n"
            " ├── [bold]uae/[/bold]  [dim]...[/dim]\n"
            " └── combined_all.csv       [dim](with --combine)[/dim]",
            title="[bold]Output Structure[/bold]",
            border_style="dim",
        )
    )
    console.print()


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="ArabLocal Universal Scraper v3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,  # We handle --help ourselves with Rich
    )

    # ── Custom help ──
    parser.add_argument("--help", "-h", action="store_true", default=False,
                        help=argparse.SUPPRESS)

    # ── Job specification (new) ──
    parser.add_argument(
        "--job", "-j", action="append", default=None,
        metavar="SPEC",
    )

    # ── Backward-compatible options ──
    parser.add_argument(
        "--country", type=str, default=None,
        choices=list(BASE_URLS.keys()),
    )
    parser.add_argument("--category", "-c", type=str, default=None)
    parser.add_argument("--limit", "-l", type=int, default=0)

    # ── Concurrency ──
    parser.add_argument("--threads", "-t", type=str, default="auto")

    # ── Proxies ──
    parser.add_argument("--proxies", "-p", type=str, default=None)

    # ── Output ──
    parser.add_argument("--output-dir", "-o", type=str, default="output")
    parser.add_argument("--combine", action="store_true")

    # ── Modes ──
    parser.add_argument("--export-only", "-e", action="store_true")
    parser.add_argument("--dry-run", "-d", action="store_true")
    parser.add_argument("--fresh", action="store_true")

    # ── Output verbosity ──
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--quiet", "-q", action="store_true")

    args = parser.parse_args()

    if args.help:
        print_rich_help()
        sys.exit(0)

    return args


def build_jobs(args) -> List[JobConfig]:
    """Convert CLI arguments into a list of JobConfig instances.

    Supports both --job (new, repeatable) and --country (legacy, single).
    """
    jobs: List[JobConfig] = []

    if args.job:
        # New multi-job mode — warn if legacy flags are also set
        if args.country or args.category or args.limit:
            console.print(
                "[yellow]Warning: --country/--category/--limit ignored when --job is used[/yellow]"
            )
        num_jobs = len(args.job)
        concurrency = resolve_concurrency(args.threads, num_jobs)

        for spec_str in args.job:
            spec = parse_job_spec(spec_str)
            job = JobConfig(
                country=spec["country"],
                categories=spec["categories"],
                limit=spec["limit"],
                output_dir=os.path.join(args.output_dir, spec["country"]),
                concurrency=concurrency,
                proxy_file=args.proxies,
                fresh=args.fresh,
                dry_run=args.dry_run,
                verbose=args.verbose,
                quiet=args.quiet,
            )
            job.validate()
            jobs.append(job)
    else:
        # Legacy single-country mode
        country = args.country or "uae"
        concurrency = resolve_concurrency(args.threads, 1)
        categories = [args.category] if args.category else []

        job = JobConfig(
            country=country,
            categories=categories,
            limit=args.limit,
            output_dir=os.path.join(args.output_dir, country),
            concurrency=concurrency,
            proxy_file=args.proxies,
            fresh=args.fresh,
            dry_run=args.dry_run,
            verbose=args.verbose,
            quiet=args.quiet,
        )
        job.validate()
        jobs.append(job)

    return jobs


def write_combined_csv(jobs: List[JobConfig], output_dir: str):
    """Merge all per-country _all.csv files into one combined CSV."""
    import sqlite3

    all_records = []
    all_keys: set = set()

    for job in jobs:
        if not os.path.exists(job.db_path):
            continue
        with sqlite3.connect(job.db_path, timeout=15) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT url, category, data FROM businesses").fetchall()
            for row in rows:
                d = json.loads(row["data"])
                d["URL"] = row["url"]
                d["Category"] = row["category"]
                d["Country_Source"] = job.country_key.upper()
                all_keys.update(d.keys())
                all_records.append(d)

    if not all_records:
        console.print("[yellow]No data found across any country databases for combined export.[/yellow]")
        return

    base_order = [
        "Country_Source", "Category", "Name", "Phone_1", "Phone_2", "Phone_3",
        "WhatsApp", "Email", "Location", "Area", "Governorate", "Country",
        "About", "Website", "Rating", "Views", "Fax", "URL",
    ]
    fieldnames = [c for c in base_order if c in all_keys]
    fieldnames += sorted(k for k in all_keys if k not in base_order)

    combined_path = os.path.join(output_dir, "combined_all.csv")
    os.makedirs(output_dir, exist_ok=True)

    with open(combined_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in all_records:
            writer.writerow({col: rec.get(col, "") for col in fieldnames})

    console.print(f"[bold green]Combined CSV: {len(all_records)} records -> {combined_path}[/bold green]")


# ─── Async Entry Point ──────────────────────────────────────────────────────

async def run_single_job(job: JobConfig):
    """Run a single country scraping job."""
    job.ensure_dirs()

    # Set up per-country log file
    from datetime import datetime
    log_file = os.path.join(
        job.log_dir,
        f"{job.country_key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    )
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(fh)

    # Load proxies
    proxies = ArabLocalEngine.load_proxies(job.proxy_file) if job.proxy_file else []

    engine = ArabLocalEngine(job, concurrency=job.concurrency, proxies=proxies)

    if job.dry_run:
        await engine.run_pipeline(
            dry_run=True,
            category_filter=",".join(job.categories) if job.categories else None,
            quiet=job.quiet,
        )
        engine.print_summary()
        log.removeHandler(fh)
        fh.close()
        return

    try:
        await engine.run_pipeline(
            fresh=job.fresh,
            category_filter=",".join(job.categories) if job.categories else None,
            quiet=job.quiet,
        )
    except (asyncio.CancelledError, KeyboardInterrupt):
        log.warning(f"[{job.country_key}] Pipeline interrupted.")
        engine.shutdown_event.set()
    finally:
        try:
            console.print(f"[bold][{job.country_key}] Exporting final CSVs…[/bold]")
            engine.storage.export_sorted_csvs()
            engine.print_summary()
        except Exception as e:
            log.error(f"[{job.country_key}] Export failed: {e}")
        log.removeHandler(fh)
        fh.close()


async def run_export_only(job: JobConfig):
    """Export CSVs from existing database without scraping."""
    if not os.path.exists(job.db_path):
        console.print(f"[yellow][{job.country_key}] No database found at {job.db_path}[/yellow]")
        return

    from core.storage import StorageManager
    storage = StorageManager(
        db_path=job.db_path,
        raw_csv_path=job.raw_csv_path,
        categories_dir=job.categories_dir,
        all_csv_path=job.all_csv_path,
    )
    job.ensure_dirs()
    storage.export_sorted_csvs()
    total = storage.get_total_businesses()
    console.print(f"[green][{job.country_key}] Exported {total} records[/green]")


async def async_main(args):
    """Main async entry point — build jobs, run in parallel."""
    # Configure console logging
    if args.verbose:
        log.addHandler(RichHandler(console=console, level=logging.DEBUG, show_path=False))
    elif not args.quiet:
        log.addHandler(RichHandler(console=console, level=logging.WARNING, show_path=False))

    # Suppress Scrapling noise
    if not args.verbose:
        logging.getLogger("scrapling").setLevel(logging.WARNING)
        # Scrapling uses root logger for "No Cloudflare challenge" messages
        for name in ("scrapling", "cdp", "browserforge", "camoufox", "rebrowser"):
            logging.getLogger(name).setLevel(logging.CRITICAL)

    jobs = build_jobs(args)

    if args.export_only:
        for job in jobs:
            await run_export_only(job)
        if args.combine:
            write_combined_csv(jobs, args.output_dir)
        return

    # Run all jobs in parallel
    if len(jobs) == 1:
        await run_single_job(jobs[0])
    else:
        console.print(
            Panel.fit(
                " | ".join(
                    f"[bold]{j.country_key.upper()}[/bold] "
                    f"limit={j.limit or '∞'} cats={','.join(j.categories) or 'ALL'}"
                    for j in jobs
                ),
                title=f"[bold blue]Parallel: {len(jobs)} countries[/bold blue]",
                border_style="blue",
            )
        )
        await asyncio.gather(
            *(run_single_job(j) for j in jobs),
            return_exceptions=True,
        )

    # Combine if requested
    if args.combine:
        write_combined_csv(jobs, args.output_dir)


def main():
    args = parse_args()
    try:
        jobs = build_jobs(args)
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

    mode = "export-only" if args.export_only else "dry-run" if args.dry_run else "full"
    job_labels = " + ".join(
        f"{j.country_key.upper()}({j.limit or '∞'})" for j in jobs
    )
    console.print(
        Panel.fit(
            f"[bold]Jobs:[/bold] {job_labels}  "
            f"[bold]Threads:[/bold] {args.threads}  "
            f"[bold]Mode:[/bold] {mode}",
            title="[bold blue]ArabLocal Scraper v3[/bold blue]",
            border_style="blue",
        )
    )

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        console.print(
            "\n[yellow][interrupted] Data saved to SQLite + raw CSV. "
            "Run again to resume, or --export-only to re-export.[/yellow]"
        )
    finally:
        # Suppress harmless asyncio/Playwright GC noise on Windows
        if sys.platform == "win32":
            import gc
            gc.collect()
            sys.stderr = open(os.devnull, "w")


if __name__ == "__main__":
    main()
