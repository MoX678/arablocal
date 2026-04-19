"""Dashboard — operator overview surface.

Taste-skill: dashboards style. Answers "what is the system doing right now?"
No marketing hero, no decorative stat cards. Instrument surface.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QGridLayout,
    QFrame, QPushButton, QScrollArea, QSizePolicy,
)

from core.config import BASE_URLS, COUNTRY_INFO


# ── Metric slab — flat inline metric, not a card ────────────────────────────

class MetricSlab(QFrame):
    """Flat metric: value + label in a horizontal strip. No card chrome."""

    def __init__(self, label: str, value: str = "0", color: str = "#34d399", parent=None):
        super().__init__(parent)
        self.setStyleSheet(
            "QFrame { background: transparent; border: none; }"
        )
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)

        self.value_lbl = QLabel(value)
        self.value_lbl.setStyleSheet(
            f"font-size: 28px; font-weight: 700; color: {color}; "
            f"font-family: 'Cascadia Code', 'Consolas', monospace; "
            f"background: transparent; border: none;"
        )
        self.value_lbl.setAlignment(Qt.AlignmentFlag.AlignLeft)

        self.label_lbl = QLabel(label.upper())
        self.label_lbl.setStyleSheet(
            "font-size: 9px; font-weight: 600; color: #56565e; "
            "letter-spacing: 1.5px; background: transparent; border: none;"
        )
        self.label_lbl.setAlignment(Qt.AlignmentFlag.AlignLeft)

        layout.addWidget(self.label_lbl)
        layout.addWidget(self.value_lbl)

    def set_value(self, value: str):
        self.value_lbl.setText(value)


# ── Country row — instrument band, not a detached card ──────────────────────

class CountryBand(QFrame):
    """Horizontal band for a country — inline data, no card elevation."""

    def __init__(self, key: str, info: dict, parent=None):
        super().__init__(parent)
        self.key = key
        self.setStyleSheet(
            "QFrame { background: #1a1a1f; border-bottom: 1px solid #252529; "
            "border-radius: 0px; }"
        )
        self.setFixedHeight(44)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(16, 0, 16, 0)
        layout.setSpacing(12)

        # Status dot
        self.dot = QLabel("\u2022")
        self.dot.setStyleSheet(
            "font-size: 14px; color: #56565e; background: transparent; border: none;"
        )
        self.dot.setFixedWidth(14)
        layout.addWidget(self.dot)

        # Country code (mono)
        code = QLabel(key.upper())
        code.setStyleSheet(
            "font-size: 11px; font-weight: 700; color: #88888f; "
            "font-family: 'Cascadia Code', monospace; background: transparent; border: none;"
        )
        code.setFixedWidth(32)
        layout.addWidget(code)

        # Country name
        name = QLabel(info["name"])
        name.setStyleSheet(
            "font-size: 13px; font-weight: 500; color: #e4e4e7; "
            "background: transparent; border: none;"
        )
        layout.addWidget(name)
        layout.addStretch()

        # Count (mono, right-aligned)
        self.count_lbl = QLabel("--")
        self.count_lbl.setStyleSheet(
            "font-size: 14px; font-weight: 700; color: #34d399; "
            "font-family: 'Cascadia Code', monospace; background: transparent; border: none;"
        )
        self.count_lbl.setAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )
        self.count_lbl.setFixedWidth(80)
        layout.addWidget(self.count_lbl)

        # Categories count (done/total)
        self.cats_lbl = QLabel("")
        self.cats_lbl.setStyleSheet(
            "font-size: 11px; color: #56565e; "
            "font-family: 'Cascadia Code', monospace; background: transparent; border: none;"
        )
        self.cats_lbl.setFixedWidth(70)
        layout.addWidget(self.cats_lbl)

    def set_counts(self, businesses: int, categories: int, done: int = 0):
        if businesses > 0:
            self.count_lbl.setText(f"{businesses:,}")
            self.dot.setStyleSheet(
                "font-size: 14px; color: #34d399; background: transparent; border: none;"
            )
        else:
            self.count_lbl.setText("--")
            self.dot.setStyleSheet(
                "font-size: 14px; color: #56565e; background: transparent; border: none;"
            )
        if categories > 0:
            remaining = categories - done
            if done > 0:
                self.cats_lbl.setText(f"{done}/{categories}")
            else:
                self.cats_lbl.setText(f"0/{categories}")
            # Color: green if all done, yellow if partial, gray if nothing
            if remaining == 0:
                color = "#34d399"
            elif done > 0:
                color = "#eab308"
            else:
                color = "#56565e"
            self.cats_lbl.setStyleSheet(
                f"font-size: 11px; color: {color}; "
                f"font-family: 'Cascadia Code', monospace; background: transparent; border: none;"
            )
        else:
            self.cats_lbl.setText("")


class DashboardPage(QWidget):
    def __init__(self, main_window=None, parent=None):
        super().__init__(parent)
        self.main_window = main_window
        self._setup_ui()
        self._timer = QTimer(self)
        self._timer.timeout.connect(self.refresh_stats)
        self._timer.start(3_000)

    def _setup_ui(self):
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)

        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(0)

        # ── Overview strip (top metrics band) ──────────────────────
        metrics_strip = QFrame()
        metrics_strip.setStyleSheet(
            "QFrame { background: #131316; border: 1px solid #252529; border-radius: 4px; }"
        )
        metrics_layout = QHBoxLayout(metrics_strip)
        metrics_layout.setContentsMargins(16, 10, 16, 10)
        metrics_layout.setSpacing(32)

        self.m_businesses = MetricSlab("Businesses", "0", "#34d399")
        self.m_categories = MetricSlab("Categories", "0", "#60a5fa")
        self.m_remaining = MetricSlab("Remaining", "0", "#f43f5e")
        self.m_countries = MetricSlab("Active", "0", "#eab308")
        self.m_csvs = MetricSlab("CSV Files", "0", "#f97316")

        metrics_layout.addWidget(self.m_businesses)
        metrics_layout.addWidget(self.m_categories)
        metrics_layout.addWidget(self.m_remaining)
        metrics_layout.addWidget(self.m_countries)
        metrics_layout.addWidget(self.m_csvs)
        metrics_layout.addStretch()

        layout.addWidget(metrics_strip)
        layout.addSpacing(14)

        # ── Currently Running card ─────────────────────────────────
        self._running_frame = QFrame()
        self._running_frame.setStyleSheet(
            "QFrame { background: #0a2e22; border: 1px solid #166d4e; border-radius: 4px; }"
        )
        running_layout = QVBoxLayout(self._running_frame)
        running_layout.setContentsMargins(14, 10, 14, 10)
        running_layout.setSpacing(6)

        running_header = QHBoxLayout()
        running_dot = QLabel("\u25CF")
        running_dot.setStyleSheet(
            "font-size: 10px; color: #34d399; background: transparent; border: none;"
        )
        running_dot.setFixedWidth(14)
        running_header.addWidget(running_dot)

        running_title = QLabel("CURRENTLY RUNNING")
        running_title.setStyleSheet(
            "font-size: 9px; font-weight: 700; color: #34d399; "
            "letter-spacing: 2px; background: transparent; border: none;"
        )
        running_header.addWidget(running_title)
        running_header.addStretch()

        self._running_elapsed = QLabel("")
        self._running_elapsed.setStyleSheet(
            "font-size: 10px; color: #56565e; "
            "font-family: 'Cascadia Code', 'Consolas', monospace; "
            "background: transparent; border: none;"
        )
        running_header.addWidget(self._running_elapsed)
        running_layout.addLayout(running_header)

        self._running_info = QLabel("")
        self._running_info.setStyleSheet(
            "font-size: 12px; color: #e4e4e7; background: transparent; border: none;"
        )
        self._running_info.setWordWrap(True)
        running_layout.addWidget(self._running_info)

        self._running_stats = QLabel("")
        self._running_stats.setStyleSheet(
            "font-size: 11px; color: #88888f; "
            "font-family: 'Cascadia Code', 'Consolas', monospace; "
            "background: transparent; border: none;"
        )
        running_layout.addWidget(self._running_stats)

        self._running_frame.setVisible(False)
        layout.addWidget(self._running_frame)
        layout.addSpacing(10)

        # ── Section label ──────────────────────────────────────────
        section_lbl = QLabel("COUNTRY STATUS")
        section_lbl.setStyleSheet(
            "font-size: 9px; font-weight: 700; color: #35353b; "
            "letter-spacing: 2px;"
        )
        layout.addWidget(section_lbl)
        layout.addSpacing(4)

        # ── Country bands (table-like list, not cards) ───────────────
        self.country_bands: dict[str, CountryBand] = {}
        for key in COUNTRY_INFO:
            band = CountryBand(key, COUNTRY_INFO[key])
            self.country_bands[key] = band
            layout.addWidget(band)

        layout.addSpacing(14)

        # ── Quick actions — compact operational row ──────────────────
        actions_lbl = QLabel("ACTIONS")
        actions_lbl.setStyleSheet(
            "font-size: 9px; font-weight: 700; color: #35353b; "
            "letter-spacing: 2px;"
        )
        layout.addWidget(actions_lbl)
        layout.addSpacing(4)

        actions_row = QHBoxLayout()
        actions_row.setSpacing(6)

        btn_scrape = QPushButton("Start Scraping")
        btn_scrape.setProperty("class", "primary")
        btn_scrape.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_scrape.setFixedHeight(30)
        btn_scrape.clicked.connect(self._go_scrape)

        btn_results = QPushButton("View Results")
        btn_results.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_results.setFixedHeight(30)
        btn_results.clicked.connect(self._go_results)

        btn_refresh = QPushButton("Refresh")
        btn_refresh.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_refresh.setFixedHeight(30)
        btn_refresh.clicked.connect(self.refresh_stats)

        actions_row.addWidget(btn_scrape)
        actions_row.addWidget(btn_results)
        actions_row.addWidget(btn_refresh)
        actions_row.addStretch()
        layout.addLayout(actions_row)

        layout.addStretch()
        scroll.setWidget(content)
        outer.addWidget(scroll)

    def showEvent(self, event):
        super().showEvent(event)
        self.refresh_stats()

    def refresh_stats(self):
        output_dir = Path("output")
        total_biz = 0
        total_cats = 0
        total_done = 0
        total_remaining = 0
        countries_found = 0
        csv_count = 0

        for key in BASE_URLS:
            db_path = output_dir / key / f"{key}_staging.db"
            biz_count = 0
            cat_count = 0
            done_count = 0
            if db_path.exists():
                try:
                    conn = sqlite3.connect(str(db_path))
                    cur = conn.cursor()
                    cur.execute("SELECT COUNT(*) FROM businesses")
                    biz_count = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM categories")
                    cat_count = cur.fetchone()[0]
                    cur.execute(
                        "SELECT COUNT(*) FROM checkpoints "
                        "WHERE completed = 1 AND urls_found > 0"
                    )
                    done_count = cur.fetchone()[0]
                    conn.close()
                    total_biz += biz_count
                    total_cats += cat_count
                    total_done += done_count
                    total_remaining += max(0, cat_count - done_count)
                    if biz_count > 0:
                        countries_found += 1
                except Exception:
                    pass

            if key in self.country_bands:
                self.country_bands[key].set_counts(biz_count, cat_count, done_count)

        if output_dir.exists():
            csv_count = sum(1 for _ in output_dir.rglob("*.csv"))

        self.m_businesses.set_value(f"{total_biz:,}")
        self.m_categories.set_value(f"{total_cats:,}")
        self.m_remaining.set_value(f"{total_remaining:,}")
        self.m_countries.set_value(str(countries_found))
        self.m_csvs.set_value(str(csv_count))

    def _go_scrape(self):
        if self.main_window:
            self.main_window._on_nav_click("Scrape")

    def _go_results(self):
        if self.main_window:
            self.main_window._on_nav_click("Results")

    def update_running_status(self, countries: list[str], categories: list[str],
                               stats: dict, elapsed: float = 0):
        """Update the 'Currently Running' card with live scrape info."""
        if not countries:
            self._running_frame.setVisible(False)
            return

        self._running_frame.setVisible(True)

        country_str = ", ".join(c.upper() for c in countries)
        cat_count = len(categories) if categories else 0
        self._running_info.setText(
            f"Countries: {country_str}  •  {cat_count} categories"
        )

        total_scraped = sum(s.scraped for s in stats.values()) if stats else 0
        total_errors = sum(s.errors for s in stats.values()) if stats else 0
        total_skipped = sum(s.skipped for s in stats.values()) if stats else 0
        self._running_stats.setText(
            f"Scraped: {total_scraped}  •  Errors: {total_errors}  •  Skipped: {total_skipped}"
        )

        if elapsed > 0:
            mins = int(elapsed // 60)
            secs = int(elapsed % 60)
            self._running_elapsed.setText(f"{mins}m {secs:02d}s")
        else:
            self._running_elapsed.setText("")

    def clear_running_status(self):
        self._running_frame.setVisible(False)
