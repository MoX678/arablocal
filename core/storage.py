"""Storage layer: SQLite database, CSV output, and checkpoint management."""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from core.config import BASE_FIELDS, SOCIAL_DOMAINS

log = logging.getLogger("arablocal")


class StorageManager:
    """Handles all persistence: SQLite staging DB, raw CSV append, and final CSV export.

    Each StorageManager owns one DB file, one raw CSV, and one output directory.
    Thread-safe via asyncio locks for concurrent producer/consumer access.
    """

    def __init__(self, db_path: str, raw_csv_path: str, categories_dir: str,
                 all_csv_path: str, start_time_ref: Optional[callable] = None):
        """
        Args:
            db_path: Path to SQLite staging database.
            raw_csv_path: Path to incremental raw CSV file.
            categories_dir: Directory for per-category exported CSVs.
            all_csv_path: Path for combined all-categories CSV.
            start_time_ref: Callable returning pipeline start time (for Runtime_Sec column).
        """
        self.db_path = db_path
        self.raw_csv_path = raw_csv_path
        self.categories_dir = categories_dir
        self.all_csv_path = all_csv_path
        self._start_time_ref = start_time_ref

        self.db_lock = asyncio.Lock()
        self.csv_lock = asyncio.Lock()

        # CSV batch buffer
        self._csv_buffer: List[dict] = []
        self._csv_fieldnames: Optional[List[str]] = None
        self._csv_flush_size = 25

        self._init_db()

    # ─── Database Initialization ──────────────────────────────────────────

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

    # ─── Business CRUD ────────────────────────────────────────────────────

    async def insert_business(self, url: str, category: str, data: dict):
        async with self.db_lock:
            with sqlite3.connect(self.db_path, timeout=15) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO businesses (url, category, data, scraped_at) VALUES (?, ?, ?, ?)",
                    (url, category, json.dumps(data, ensure_ascii=False),
                     datetime.now(timezone.utc).isoformat()),
                )

    async def insert_category(self, slug: str, name: str, url: str):
        async with self.db_lock:
            with sqlite3.connect(self.db_path, timeout=15) as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO categories (slug, name, url, discovered_at) VALUES (?, ?, ?, ?)",
                    (slug, name, url, datetime.now(timezone.utc).isoformat()),
                )

    def get_existing_urls(self, category: str) -> Set[str]:
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            rows = conn.execute(
                "SELECT url FROM businesses WHERE category = ?", (category,)
            ).fetchall()
            return {r[0] for r in rows}

    def get_total_businesses(self) -> int:
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            return conn.execute("SELECT COUNT(*) FROM businesses").fetchone()[0]

    def get_total_categories(self) -> int:
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            return conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]

    # ─── Checkpoints ──────────────────────────────────────────────────────

    async def checkpoint_save(self, slug: str, page: int, urls_found: int,
                              completed: bool = False):
        async with self.db_lock:
            with sqlite3.connect(self.db_path, timeout=15) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO checkpoints "
                    "(category_slug, last_page, urls_found, completed, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (slug, page, urls_found, int(completed),
                     datetime.now(timezone.utc).isoformat()),
                )

    def checkpoint_get(self, slug: str) -> Optional[dict]:
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM checkpoints WHERE category_slug = ?", (slug,)
            ).fetchone()
            return dict(row) if row else None

    def checkpoint_summary(self) -> dict:
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            completed = conn.execute(
                "SELECT COUNT(*) FROM checkpoints WHERE completed = 1"
            ).fetchone()[0]
            in_progress = conn.execute(
                "SELECT COUNT(*) FROM checkpoints WHERE completed = 0"
            ).fetchone()[0]
            return {"completed": completed, "in_progress": in_progress}

    def checkpoint_clear(self):
        with sqlite3.connect(self.db_path, timeout=15) as conn:
            conn.execute("DELETE FROM checkpoints")
            conn.commit()

    # ─── Raw CSV (batched append) ─────────────────────────────────────────

    async def append_raw_csv(self, data: dict, category: str, url: str):
        """Buffer a business row and flush to CSV when buffer is full."""
        async with self.csv_lock:
            try:
                elapsed = 0.0
                if self._start_time_ref:
                    elapsed = self._start_time_ref()
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
            file_exists = os.path.exists(self.raw_csv_path)
            with open(self.raw_csv_path, "a", newline="", encoding="utf-8-sig") as fh:
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

    # ─── Final Export: Priority-Sorted CSVs ───────────────────────────────

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

            base_order = ["Name", "Phone_1", "Phone_2", "Phone_3", "WhatsApp",
                          "Email", "Location", "Area",
                          "Governorate", "Country", "About", "Website",
                          "Rating", "Views", "Fax", "URL"]

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

                fieldnames = [c for c in base_order if c in all_keys]
                fieldnames += sorted(k for k in all_keys if k not in base_order)

                # Priority sort: businesses needing digital presence ranked first
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
                filename = os.path.join(self.categories_dir, f"{safe_name}.csv")

                with open(filename, "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for rec in records:
                        writer.writerow({col: rec.get(col, "") for col in fieldnames})

                log.info(f"Exported {len(records)} records -> {filename} (priority sorted)")

            # Combined file
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

                with open(self.all_csv_path, "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=combined_order)
                    writer.writeheader()
                    for rec in all_records:
                        writer.writerow({col: rec.get(col, "") for col in combined_order})

                log.info(f"Exported {len(all_records)} total records -> {self.all_csv_path}")
