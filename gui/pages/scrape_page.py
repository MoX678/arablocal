"""Scrape Command Center — configure jobs, select countries/categories, monitor progress."""

from __future__ import annotations

import os
import time
from datetime import datetime

from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QComboBox, QSpinBox, QLineEdit, QCheckBox, QGroupBox,
    QGridLayout, QTextEdit, QProgressBar, QFrame, QSplitter,
    QScrollArea, QSizePolicy,
)

from core.config import COUNTRY_INFO, BASE_URLS, JobConfig, resolve_concurrency
from core.cookie_manager import get_cookie_manager
from gui.workers import ScrapeWorker, ScrapeStats, MultiCategoryFetchWorker, QtLogHandler


class CountryToggle(QPushButton):
    """Checkable toggle button for a country."""

    def __init__(self, key: str, name: str, parent=None):
        super().__init__(f"{name}", parent)
        self.key = key
        self.setCheckable(True)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setProperty("class", "")
        self.setMinimumHeight(36)
        self._update_style()
        self.toggled.connect(lambda: self._update_style())

    def _update_style(self):
        if self.isChecked():
            self.setStyleSheet(
                "QPushButton { background: #0a2e22; color: #34d399; "
                "border: 1px solid #166d4e; border-radius: 4px; "
                "padding: 5px 12px; font-weight: 700; font-size: 11px; }"
            )
        else:
            self.setStyleSheet(
                "QPushButton { background: #131316; color: #56565e; "
                "border: 1px solid #252529; border-radius: 4px; "
                "padding: 5px 12px; font-weight: 500; font-size: 11px; }"
                "QPushButton:hover { border-color: #166d4e; color: #e4e4e7; }"
            )


class CategoryCheckbox(QCheckBox):
    """Checkbox for a single category."""

    def __init__(self, slug: str, name: str, parent=None):
        super().__init__(name, parent)
        self.slug = slug
        self.setChecked(True)


class CategoryProgressItem(QFrame):
    """Per-category progress row — shows page progress and estimated items."""

    def __init__(self, cat_name: str, parent=None):
        super().__init__(parent)
        self.cat_name = cat_name
        self._total_pages = 0
        self._current_page = 0
        self._total_urls = 0

        self.setStyleSheet(
            "QFrame { background: transparent; border: none; }"
        )
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 1, 0, 1)
        layout.setSpacing(6)

        self.lbl_name = QLabel(cat_name[:28])
        self.lbl_name.setFixedWidth(160)
        self.lbl_name.setStyleSheet(
            "font-size: 10px; color: #88888f; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        layout.addWidget(self.lbl_name)

        self.progress = QProgressBar()
        self.progress.setFixedHeight(3)
        self.progress.setTextVisible(False)
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        layout.addWidget(self.progress, stretch=1)

        self.lbl_info = QLabel("")
        self.lbl_info.setStyleSheet(
            "font-size: 9px; color: #56565e; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        self.lbl_info.setFixedWidth(120)
        self.lbl_info.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        layout.addWidget(self.lbl_info)

        self.setFixedHeight(16)

    def update_progress(self, page_num: int, total_pages: int, new_urls: int):
        self._current_page = page_num
        self._total_urls += new_urls
        if total_pages > 0:
            self._total_pages = total_pages
            pct = min(int(page_num / total_pages * 100), 100)
            self.progress.setValue(pct)
            est_items = total_pages * 20
            self.lbl_info.setText(f"p{page_num}/{total_pages} · ~{est_items} items")
        else:
            self.progress.setRange(0, 0)  # indeterminate
            self.lbl_info.setText(f"p{page_num} · {self._total_urls} urls")

    def set_completed(self):
        self.progress.setRange(0, 100)
        self.progress.setValue(100)
        self.lbl_name.setStyleSheet(
            "font-size: 10px; color: #34d399; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )


class CountryProgressBar(QFrame):
    """Per-country progress bar with stats, per-category breakdown, and stop button."""

    country_stop_requested = None  # Set by ScrapePage as a callback

    def __init__(self, key: str, parent=None):
        super().__init__(parent)
        self.key = key
        self._category_items: dict[str, CategoryProgressItem] = {}
        self._cats_expanded = True
        self.setStyleSheet(
            "QFrame { background: #131316; border: 1px solid #252529; border-radius: 4px; }"
        )
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 6, 10, 6)
        layout.setSpacing(3)

        # Top row: country name + stats + stop button
        top = QHBoxLayout()
        top.setSpacing(12)

        self.lbl_country = QLabel(key.upper())
        self.lbl_country.setStyleSheet(
            "font-weight: 700; font-size: 12px; color: #34d399; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        top.addWidget(self.lbl_country)

        self.lbl_status = QLabel("Waiting...")
        self.lbl_status.setStyleSheet("font-size: 11px; color: #56565e;")
        top.addWidget(self.lbl_status)
        top.addStretch()

        self.lbl_scraped = QLabel("0")
        self.lbl_scraped.setStyleSheet(
            "font-weight: 700; color: #34d399; font-family: 'Cascadia Code', 'Consolas', monospace; font-size: 13px;"
        )
        top.addWidget(self.lbl_scraped)

        self.lbl_rate = QLabel("")
        self.lbl_rate.setStyleSheet(
            "color: #60a5fa; font-family: 'Cascadia Code', 'Consolas', monospace; font-size: 11px;"
        )
        top.addWidget(self.lbl_rate)

        self.lbl_elapsed = QLabel("")
        self.lbl_elapsed.setStyleSheet(
            "color: #56565e; font-family: 'Cascadia Code', 'Consolas', monospace; font-size: 11px;"
        )
        top.addWidget(self.lbl_elapsed)

        # Per-country stop button
        self.btn_stop = QPushButton("X")
        self.btn_stop.setObjectName("countryStop")
        stop_font = QFont("Cascadia Code", 10)
        stop_font.setBold(True)
        self.btn_stop.setFont(stop_font)
        self.btn_stop.setFixedSize(32, 24)
        self.btn_stop.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_stop.setToolTip(f"Stop {key.upper()}")
        self.btn_stop.setStyleSheet(
            "#countryStop { background: #2a1215; color: #f87171; border: 1px solid #7f1d1d; "
            "border-radius: 3px; padding: 0px; margin: 0px; }"
            "#countryStop:hover { background: #991b1b; color: #fca5a5; }"
        )
        self.btn_stop.clicked.connect(self._on_stop_clicked)
        top.addWidget(self.btn_stop)

        layout.addLayout(top)

        # Progress bar
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setFixedHeight(4)
        self.progress.setTextVisible(False)
        layout.addWidget(self.progress)

        # Category progress container in scrollable area
        self._cat_scroll = QScrollArea()
        self._cat_scroll.setWidgetResizable(True)
        self._cat_scroll.setMaximumHeight(120)
        self._cat_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self._cat_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._cat_scroll.setStyleSheet(
            "QScrollArea { background: transparent; border: none; }"
            "QScrollBar:vertical { width: 4px; background: transparent; }"
            "QScrollBar::handle:vertical { background: #35353b; border-radius: 2px; }"
            "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0px; }"
        )
        self._cat_container = QWidget()
        self._cat_container.setStyleSheet("QWidget { background: transparent; }")
        self._cat_layout = QVBoxLayout(self._cat_container)
        self._cat_layout.setContentsMargins(4, 4, 0, 0)
        self._cat_layout.setSpacing(1)
        self._cat_scroll.setWidget(self._cat_container)
        layout.addWidget(self._cat_scroll)

    def _on_stop_clicked(self):
        if self.country_stop_requested:
            self.country_stop_requested(self.key)
        self.btn_stop.setEnabled(False)
        self.lbl_status.setText("Stopping...")
        self.lbl_status.setStyleSheet("font-size: 11px; color: #eab308;")

    def update_category_progress(self, cat_name: str, page_num: int, total_pages: int, new_urls: int):
        if cat_name not in self._category_items:
            item = CategoryProgressItem(cat_name)
            self._category_items[cat_name] = item
            self._cat_layout.addWidget(item)
            # Auto-scroll to newest item
            sb = self._cat_scroll.verticalScrollBar()
            sb.setValue(sb.maximum())
        self._category_items[cat_name].update_progress(page_num, total_pages, new_urls)

    def set_started(self):
        self.lbl_status.setText("Running...")
        self.lbl_status.setStyleSheet("font-size: 11px; color: #eab308;")
        self.progress.setRange(0, 0)  # indeterminate

    def update_stats(self, stats: ScrapeStats):
        self.lbl_scraped.setText(str(stats.scraped))
        if stats.rate > 0:
            self.lbl_rate.setText(f"{stats.rate:.1f}/m")
        if stats.elapsed > 0:
            mins = int(stats.elapsed // 60)
            secs = int(stats.elapsed % 60)
            self.lbl_elapsed.setText(f"{mins}m{secs:02d}s")

        # Update progress
        if hasattr(self, '_limit') and self._limit > 0:
            pct = min(int(stats.scraped / self._limit * 100), 100)
            self.progress.setRange(0, 100)
            self.progress.setValue(pct)

    def set_completed(self, stats: ScrapeStats):
        self.lbl_status.setText("Done")
        self.lbl_status.setStyleSheet("font-size: 11px; color: #34d399;")
        self.progress.setRange(0, 100)
        self.progress.setValue(100)
        self.btn_stop.setEnabled(False)
        self.btn_stop.setStyleSheet(
            "#countryStop { background: #1a1a1f; color: #35353b; border: 1px solid #252529; "
            "border-radius: 3px; padding: 0px; margin: 0px; }"
        )
        # Mark all category items as completed
        for item in self._category_items.values():
            item.set_completed()

    def set_error(self, msg: str):
        self.lbl_status.setText(f"Error: {msg[:50]}")
        self.lbl_status.setStyleSheet("font-size: 11px; color: #f87171;")
        self.progress.setRange(0, 100)
        self.progress.setValue(0)

    def set_limit(self, limit: int):
        self._limit = limit


class ScrapePage(QWidget):
    """Main command center: country/category selection, config, live monitoring."""

    def __init__(self, main_window=None, parent=None):
        super().__init__(parent)
        self.main_window = main_window
        self._worker: ScrapeWorker | None = None
        self._workers: list[ScrapeWorker] = []
        self._running_countries: set[str] = set()
        self._cat_worker: MultiCategoryFetchWorker | None = None
        self._is_running = False
        self._qt_log_handler = None
        self._categories_cache: dict[str, list] = {}
        self._country_progress: dict[str, CountryProgressBar] = {}
        self._category_checkboxes: list[CategoryCheckbox] = []
        self._setup_ui()

    def _setup_ui(self):
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        # ── Splitter for config / monitor ────────────────────────────
        self._splitter = QSplitter(Qt.Orientation.Horizontal)
        self._splitter.setHandleWidth(1)
        self._splitter.setChildrenCollapsible(False)

        # ════════════════ LEFT PANEL: Configuration ════════════════════
        left_frame = QFrame()
        left_frame.setStyleSheet(
            "QFrame { background: #1a1a1f; border-right: 1px solid #252529; }"
        )
        left_frame.setMinimumWidth(340)

        left_scroll = QScrollArea()
        left_scroll.setWidgetResizable(True)
        left_scroll.setFrameShape(QFrame.Shape.NoFrame)

        left_widget = QWidget()
        left_widget.setStyleSheet("QWidget { background: transparent; }")
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(16, 14, 14, 14)
        left_layout.setSpacing(10)

        # Header
        header = QLabel("SCRAPE")
        header.setStyleSheet(
            "font-size: 10px; font-weight: 700; color: #56565e; "
            "letter-spacing: 2px; padding-bottom: 4px; "
            "border-bottom: 1px solid #252529;"
        )
        left_layout.addWidget(header)

        # ── Country Selection ────────────────────────────────────────
        section_countries = QLabel("COUNTRIES")
        section_countries.setStyleSheet(
            "font-size: 9px; font-weight: 700; color: #56565e; "
            "letter-spacing: 1.5px; margin-top: 4px;"
        )
        left_layout.addWidget(section_countries)

        country_row = QHBoxLayout()
        country_row.setSpacing(6)
        self.country_toggles: dict[str, CountryToggle] = {}

        for key, info in COUNTRY_INFO.items():
            toggle = CountryToggle(key, info["name"])
            toggle.toggled.connect(self._on_country_toggled)
            self.country_toggles[key] = toggle
            country_row.addWidget(toggle)

        left_layout.addLayout(country_row)

        # Fetch categories button (optional preview)
        cat_action_row = QHBoxLayout()
        cat_action_row.setSpacing(4)

        self.btn_fetch_cats = QPushButton("Preview Categories")
        self.btn_fetch_cats.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_fetch_cats.setEnabled(False)
        self.btn_fetch_cats.setToolTip("Optional: preview available categories before scraping")
        self.btn_fetch_cats.clicked.connect(self._fetch_categories)
        cat_action_row.addWidget(self.btn_fetch_cats, stretch=1)

        self.btn_select_all_cats = QPushButton("All")
        self.btn_select_all_cats.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_select_all_cats.setFixedWidth(50)
        self.btn_select_all_cats.clicked.connect(self._select_all_cats)
        cat_action_row.addWidget(self.btn_select_all_cats)

        self.btn_clear_cats = QPushButton("None")
        self.btn_clear_cats.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_clear_cats.setFixedWidth(56)
        self.btn_clear_cats.clicked.connect(self._clear_cats)
        cat_action_row.addWidget(self.btn_clear_cats)

        left_layout.addLayout(cat_action_row)

        self.cat_fetch_status = QLabel("")
        self.cat_fetch_status.setStyleSheet("font-size: 11px; color: #56565e;")
        left_layout.addWidget(self.cat_fetch_status)

        # Category checkboxes area (scrollable)
        self.cat_scroll = QScrollArea()
        self.cat_scroll.setWidgetResizable(True)
        self.cat_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.cat_scroll.setMaximumHeight(180)
        self.cat_scroll.setStyleSheet(
            "QScrollArea { background: #131316; border: 1px solid #252529; border-radius: 4px; }"
        )

        self.cat_container = QWidget()
        self.cat_layout = QVBoxLayout(self.cat_container)
        self.cat_layout.setContentsMargins(8, 6, 8, 6)
        self.cat_layout.setSpacing(2)

        self.cat_placeholder = QLabel("Categories auto-discovered on Start (or preview first)")
        self.cat_placeholder.setStyleSheet("color: #56565e; font-size: 11px; padding: 8px;")
        self.cat_layout.addWidget(self.cat_placeholder)
        self.cat_layout.addStretch()

        self.cat_scroll.setWidget(self.cat_container)
        left_layout.addWidget(self.cat_scroll)

        # ── Cookie Status Indicator ──────────────────────────────────
        cookie_row = QHBoxLayout()
        cookie_row.setSpacing(6)

        cookie_lbl = QLabel("CF COOKIES")
        cookie_lbl.setStyleSheet(
            "font-size: 9px; font-weight: 600; color: #56565e; letter-spacing: 0.5px;"
        )
        cookie_row.addWidget(cookie_lbl)

        self.cookie_status_label = QLabel("Not solved")
        self.cookie_status_label.setStyleSheet(
            "font-size: 10px; color: #56565e; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        cookie_row.addWidget(self.cookie_status_label)

        self.cookie_dot = QLabel("●")
        self.cookie_dot.setStyleSheet("font-size: 10px; color: #56565e;")
        self.cookie_dot.setFixedWidth(14)
        cookie_row.addWidget(self.cookie_dot)
        cookie_row.addStretch()

        left_layout.addLayout(cookie_row)

        # ── Job Configuration ────────────────────────────────────────
        section_config = QLabel("CONFIGURATION")
        section_config.setStyleSheet(
            "font-size: 9px; font-weight: 700; color: #35353b; "
            "letter-spacing: 2px; margin-top: 4px;"
        )
        left_layout.addWidget(section_config)

        config_grid = QGridLayout()
        config_grid.setSpacing(6)
        config_grid.setColumnStretch(1, 1)
        config_grid.setColumnStretch(3, 1)

        # Limit
        lbl_limit = QLabel("LIMIT")
        lbl_limit.setStyleSheet(
            "font-size: 9px; font-weight: 600; color: #56565e; letter-spacing: 0.5px;"
        )
        config_grid.addWidget(lbl_limit, 0, 0)
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(0, 100_000)
        self.limit_spin.setValue(0)
        self.limit_spin.setSpecialValueText("Unlimited")
        config_grid.addWidget(self.limit_spin, 0, 1)

        # Threads
        lbl_threads = QLabel("THREADS")
        lbl_threads.setStyleSheet(
            "font-size: 9px; font-weight: 600; color: #56565e; letter-spacing: 0.5px;"
        )
        config_grid.addWidget(lbl_threads, 0, 2)
        self.threads_combo = QComboBox()
        self.threads_combo.addItems(["auto", "max"])
        for i in range(1, min(os.cpu_count() * 2 + 1, 17)):
            self.threads_combo.addItem(str(i))
        config_grid.addWidget(self.threads_combo, 0, 3)

        # Output dir
        lbl_output = QLabel("OUTPUT")
        lbl_output.setStyleSheet(
            "font-size: 9px; font-weight: 600; color: #56565e; letter-spacing: 0.5px;"
        )
        config_grid.addWidget(lbl_output, 1, 0)
        self.output_input = QLineEdit("output")
        self.output_input.setStyleSheet(
            "font-size: 11px; font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        config_grid.addWidget(self.output_input, 1, 1, 1, 3)

        left_layout.addLayout(config_grid)

        # Flags - two rows for better layout
        flags_row1 = QHBoxLayout()
        flags_row1.setSpacing(10)
        self.fresh_check = QCheckBox("Fresh start")
        self.fresh_check.setToolTip("Clear checkpoints, scrape from scratch")
        self.dry_run_check = QCheckBox("Dry run")
        self.dry_run_check.setToolTip("Discover categories only, no scraping")
        self.verbose_check = QCheckBox("Verbose")
        self.verbose_check.setToolTip("Show detailed logging output")
        flags_row1.addWidget(self.fresh_check)
        flags_row1.addWidget(self.dry_run_check)
        flags_row1.addWidget(self.verbose_check)
        flags_row1.addStretch()
        left_layout.addLayout(flags_row1)

        flags_row2 = QHBoxLayout()
        flags_row2.setSpacing(10)
        self.combine_check = QCheckBox("Combine CSVs")
        self.combine_check.setToolTip("Merge all countries into combined_all.csv")
        self.combine_check.setChecked(True)
        flags_row2.addWidget(self.combine_check)
        flags_row2.addStretch()
        left_layout.addLayout(flags_row2)

        # ── Proxy Section ─────────────────────────────────────────
        proxy_row = QHBoxLayout()
        proxy_row.setSpacing(8)

        self.proxy_toggle = QCheckBox("Use Proxies")
        self.proxy_toggle.setToolTip("Route traffic through proxy pool")
        self.proxy_toggle.setChecked(False)
        self.proxy_toggle.toggled.connect(self._update_proxy_indicator)
        proxy_row.addWidget(self.proxy_toggle)

        self.proxy_indicator = QLabel("")
        self.proxy_indicator.setStyleSheet(
            "font-size: 10px; color: #56565e; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        proxy_row.addWidget(self.proxy_indicator, 1)

        self.btn_proxy_config = QPushButton("Configure →")
        self.btn_proxy_config.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_proxy_config.setStyleSheet(
            "QPushButton { background: transparent; color: #60a5fa; "
            "border: none; font-size: 11px; font-weight: 600; padding: 2px 6px; }"
            "QPushButton:hover { color: #93c5fd; text-decoration: underline; }"
        )
        self.btn_proxy_config.clicked.connect(self._go_to_proxy_page)
        proxy_row.addWidget(self.btn_proxy_config)

        left_layout.addLayout(proxy_row)
        self._update_proxy_indicator()

        # ── Action Buttons ───────────────────────────────────────────
        btn_row = QHBoxLayout()
        btn_row.setSpacing(6)

        self.btn_start = QPushButton("▶  Start Scraping")
        self.btn_start.setProperty("class", "primary")
        self.btn_start.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_start.setMinimumHeight(36)
        self.btn_start.setToolTip("Start: auto-solves CF, discovers categories, then scrapes")
        self.btn_start.clicked.connect(self._start_scrape)
        btn_row.addWidget(self.btn_start, 1)

        self.btn_stop = QPushButton("■  Stop All")
        self.btn_stop.setProperty("class", "danger")
        self.btn_stop.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_stop.setEnabled(False)
        self.btn_stop.setMinimumHeight(36)
        self.btn_stop.clicked.connect(self._stop_scrape)
        btn_row.addWidget(self.btn_stop, 1)

        left_layout.addLayout(btn_row)
        left_layout.addStretch()

        left_scroll.setWidget(left_widget)
        left_frame_layout = QVBoxLayout(left_frame)
        left_frame_layout.setContentsMargins(0, 0, 0, 0)
        left_frame_layout.addWidget(left_scroll)
        self._splitter.addWidget(left_frame)

        # ════════════════ RIGHT PANEL: Live Monitor ═══════════════════
        self._monitor_widget = QWidget()
        right_layout = QVBoxLayout(self._monitor_widget)
        right_layout.setContentsMargins(14, 14, 16, 14)
        right_layout.setSpacing(8)

        # Monitor header with layout toggle + close
        mon_header_row = QHBoxLayout()
        mon_header_row.setSpacing(6)

        monitor_header = QLabel("MONITOR")
        monitor_header.setStyleSheet(
            "font-size: 10px; font-weight: 700; color: #56565e; "
            "letter-spacing: 2px; padding-bottom: 4px; "
            "border-bottom: 1px solid #252529;"
        )
        mon_header_row.addWidget(monitor_header)
        mon_header_row.addStretch()

        # Layout toggle buttons
        self._btn_vertical = QPushButton("⊞")
        self._btn_vertical.setFixedSize(26, 26)
        self._btn_vertical.setToolTip("Side-by-side layout")
        self._btn_vertical.setCursor(Qt.CursorShape.PointingHandCursor)
        self._btn_vertical.setStyleSheet(
            "QPushButton { background: #252529; color: #88888f; border: none; "
            "border-radius: 4px; font-size: 12px; font-weight: 700; }"
            "QPushButton:hover { background: #35353b; color: #e4e4e7; }"
        )
        self._btn_vertical.clicked.connect(lambda: self._set_layout_mode("vertical"))
        mon_header_row.addWidget(self._btn_vertical)

        self._btn_horizontal = QPushButton("▬")
        self._btn_horizontal.setFixedSize(26, 26)
        self._btn_horizontal.setToolTip("Stacked layout")
        self._btn_horizontal.setCursor(Qt.CursorShape.PointingHandCursor)
        self._btn_horizontal.setStyleSheet(
            "QPushButton { background: #1a1a1f; color: #56565e; border: none; "
            "border-radius: 4px; font-size: 12px; font-weight: 700; }"
            "QPushButton:hover { background: #35353b; color: #e4e4e7; }"
        )
        self._btn_horizontal.clicked.connect(lambda: self._set_layout_mode("horizontal"))
        mon_header_row.addWidget(self._btn_horizontal)

        # Close monitor button
        self._btn_close_monitor = QPushButton("✕")
        self._btn_close_monitor.setFixedSize(26, 26)
        self._btn_close_monitor.setToolTip("Hide monitor")
        self._btn_close_monitor.setCursor(Qt.CursorShape.PointingHandCursor)
        self._btn_close_monitor.setStyleSheet(
            "QPushButton { background: transparent; color: #56565e; border: none; "
            "border-radius: 4px; font-size: 11px; }"
            "QPushButton:hover { background: #35353b; color: #f87171; }"
        )
        self._btn_close_monitor.clicked.connect(self._hide_monitor)
        mon_header_row.addWidget(self._btn_close_monitor)

        right_layout.addLayout(mon_header_row)

        # ── Per-country progress bars ────────────────────────────────
        self.progress_container = QVBoxLayout()
        self.progress_container.setSpacing(6)

        self.progress_placeholder = QLabel("No active jobs")
        self.progress_placeholder.setStyleSheet(
            "color: #35353b; font-size: 11px; padding: 6px; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        self.progress_container.addWidget(self.progress_placeholder)

        right_layout.addLayout(self.progress_container)

        # ── Aggregate Stats Bar ──────────────────────────────────────
        stats_frame = QFrame()
        stats_frame.setStyleSheet(
            "QFrame { background: #131316; "
            "border: 1px solid #252529; border-radius: 4px; }"
        )
        stats_layout = QHBoxLayout(stats_frame)
        stats_layout.setContentsMargins(10, 6, 10, 6)
        stats_layout.setSpacing(16)

        self.lbl_total_scraped = QLabel("0")
        self.lbl_total_scraped.setStyleSheet(
            "font-weight: 700; font-size: 14px; color: #34d399; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        lbl_scraped_label = QLabel("SCRAPED")
        lbl_scraped_label.setStyleSheet("font-size: 8px; color: #35353b; letter-spacing: 1px;")

        self.lbl_total_errors = QLabel("0")
        self.lbl_total_errors.setStyleSheet(
            "font-weight: 700; font-size: 14px; color: #f87171; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        lbl_errors_label = QLabel("ERRORS")
        lbl_errors_label.setStyleSheet("font-size: 8px; color: #35353b; letter-spacing: 1px;")

        self.lbl_total_skipped = QLabel("0")
        self.lbl_total_skipped.setStyleSheet(
            "font-weight: 700; font-size: 14px; color: #eab308; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        lbl_skipped_label = QLabel("SKIPPED")
        lbl_skipped_label.setStyleSheet("font-size: 8px; color: #35353b; letter-spacing: 1px;")

        for lbl, sub in [(self.lbl_total_scraped, lbl_scraped_label),
                         (self.lbl_total_errors, lbl_errors_label),
                         (self.lbl_total_skipped, lbl_skipped_label)]:
            col = QVBoxLayout()
            col.setSpacing(0)
            col.addWidget(lbl, alignment=Qt.AlignmentFlag.AlignCenter)
            col.addWidget(sub, alignment=Qt.AlignmentFlag.AlignCenter)
            stats_layout.addLayout(col)

        stats_layout.addStretch()
        right_layout.addWidget(stats_frame)

        # ── Live Log ─────────────────────────────────────────────────
        log_header = QLabel("LOG")
        log_header.setStyleSheet(
            "font-size: 9px; font-weight: 700; color: #35353b; letter-spacing: 2px;"
        )
        right_layout.addWidget(log_header)

        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setMinimumHeight(160)
        right_layout.addWidget(self.log_output, stretch=1)

        self._splitter.addWidget(self._monitor_widget)
        self._splitter.setStretchFactor(0, 1)
        self._splitter.setStretchFactor(1, 2)

        outer.addWidget(self._splitter)

        # Start with monitor hidden
        self._monitor_widget.setVisible(False)
        self._layout_mode = "vertical"

        # Dashboard refresh timer (fires while scraping)
        self._dashboard_timer = QTimer(self)
        self._dashboard_timer.setInterval(2000)
        self._dashboard_timer.timeout.connect(self._refresh_dashboard)

        # Stats accumulator
        self._stats_per_country: dict[str, ScrapeStats] = {}

    # ─── Country toggling ────────────────────────────────────────────────

    def _on_country_toggled(self):
        selected = self._get_selected_countries()
        self.btn_fetch_cats.setEnabled(len(selected) > 0)
        self._update_proxy_indicator()

    def _get_selected_countries(self) -> list[str]:
        return [k for k, t in self.country_toggles.items() if t.isChecked()]

    # ─── Category discovery ──────────────────────────────────────────────

    def _fetch_categories(self):
        countries = self._get_selected_countries()
        if not countries:
            return

        # Check which need fetching
        to_fetch = [c for c in countries if c not in self._categories_cache]
        already_cached = [c for c in countries if c in self._categories_cache]

        # Show cached immediately
        if already_cached:
            self._rebuild_category_checkboxes()

        if not to_fetch:
            self.cat_fetch_status.setText("All categories loaded from cache")
            self.cat_fetch_status.setStyleSheet("font-size: 11px; color: #34d399;")
            return

        self.btn_fetch_cats.setEnabled(False)
        self.cat_fetch_status.setText(f"Discovering categories for {', '.join(c.upper() for c in to_fetch)}...")
        self.cat_fetch_status.setStyleSheet("font-size: 11px; color: #eab308;")

        self._cat_worker = MultiCategoryFetchWorker(to_fetch)
        self._cat_worker.categories_ready.connect(self._on_cats_ready)
        self._cat_worker.country_error.connect(self._on_cat_error)
        self._cat_worker.discovery_log.connect(self._on_log_message)
        self._cat_worker.all_finished.connect(self._on_cats_done)
        self._cat_worker.start()

    def _on_cats_ready(self, country: str, cats: list):
        self._categories_cache[country] = cats
        self._rebuild_category_checkboxes()

    def _on_cat_error(self, country: str, error: str):
        self._log(f"[{country.upper()}] Category fetch failed: {error}", "#f87171")

    def _on_cats_done(self):
        self.btn_fetch_cats.setEnabled(True)
        total = sum(len(self._categories_cache.get(c, [])) for c in self._get_selected_countries())
        self.cat_fetch_status.setText(f"{total} categories discovered")
        self.cat_fetch_status.setStyleSheet("font-size: 11px; color: #34d399;")

    def _rebuild_category_checkboxes(self):
        """Rebuild the category checkbox list from cache for selected countries."""
        # Clear existing
        for cb in self._category_checkboxes:
            cb.setParent(None)
            cb.deleteLater()
        self._category_checkboxes.clear()

        if self.cat_placeholder.parent():
            self.cat_placeholder.setParent(None)

        # Remove stretch
        while self.cat_layout.count():
            item = self.cat_layout.takeAt(0)
            if item.widget():
                item.widget().setParent(None)

        selected = self._get_selected_countries()
        any_cats = False

        for country in selected:
            cats = self._categories_cache.get(country, [])
            if not cats:
                continue
            any_cats = True

            # Country header
            if len(selected) > 1:
                header = QLabel(COUNTRY_INFO[country]["name"].upper())
                header.setStyleSheet(
                    "font-size: 9px; font-weight: 700; color: #56565e; "
                    "letter-spacing: 1px; margin-top: 4px;"
                )
                self.cat_layout.addWidget(header)

            for cat in cats:
                slug = cat.get("slug", cat.get("name", "unknown"))
                name = cat.get("name", slug)
                cb = CategoryCheckbox(slug, name)
                self._category_checkboxes.append(cb)
                self.cat_layout.addWidget(cb)

        if not any_cats:
            self.cat_layout.addWidget(self.cat_placeholder)

        self.cat_layout.addStretch()

    def _select_all_cats(self):
        for cb in self._category_checkboxes:
            cb.setChecked(True)

    def _clear_cats(self):
        for cb in self._category_checkboxes:
            cb.setChecked(False)

    def _get_selected_categories(self) -> list[str]:
        return [cb.slug for cb in self._category_checkboxes if cb.isChecked()]

    # ─── Proxy indicator ─────────────────────────────────────────────────

    # ─── Monitor visibility & layout ───────────────────────────────────

    def _show_monitor(self):
        self._monitor_widget.setVisible(True)

    def _hide_monitor(self):
        self._monitor_widget.setVisible(False)

    def _set_layout_mode(self, mode: str):
        self._layout_mode = mode
        if mode == "vertical":
            self._splitter.setOrientation(Qt.Orientation.Horizontal)
            self._btn_vertical.setStyleSheet(
                "QPushButton { background: #252529; color: #88888f; border: none; "
                "border-radius: 4px; font-size: 12px; font-weight: 700; }"
            )
            self._btn_horizontal.setStyleSheet(
                "QPushButton { background: #1a1a1f; color: #56565e; border: none; "
                "border-radius: 4px; font-size: 12px; font-weight: 700; }"
                "QPushButton:hover { background: #35353b; color: #e4e4e7; }"
            )
        else:
            self._splitter.setOrientation(Qt.Orientation.Vertical)
            self._btn_horizontal.setStyleSheet(
                "QPushButton { background: #252529; color: #88888f; border: none; "
                "border-radius: 4px; font-size: 12px; font-weight: 700; }"
            )
            self._btn_vertical.setStyleSheet(
                "QPushButton { background: #1a1a1f; color: #56565e; border: none; "
                "border-radius: 4px; font-size: 12px; font-weight: 700; }"
                "QPushButton:hover { background: #35353b; color: #e4e4e7; }"
            )

    def _refresh_dashboard(self):
        if self.main_window:
            dashboard = self.main_window.get_page("Dashboard")
            if dashboard:
                dashboard.refresh_stats()
                if self._is_running:
                    countries = self._get_selected_countries()
                    categories = [cb.slug for cb in self._category_checkboxes if cb.isChecked()]
                    elapsed = time.time() - self._start_time if hasattr(self, '_start_time') else 0
                    dashboard.update_running_status(
                        countries, categories, self._stats_per_country, elapsed
                    )

    # ─── Proxy helpers ───────────────────────────────────────────────────

    def _update_proxy_indicator(self, *_args):
        summary = self._get_proxy_summary()
        if summary["total"] == 0:
            self.proxy_indicator.setText("No proxies loaded")
            self.proxy_indicator.setStyleSheet(
                "font-size: 10px; color: #56565e; "
                "font-family: 'Cascadia Code', 'Consolas', monospace;"
            )
        elif summary["tested"]:
            self.proxy_indicator.setText(
                f"{summary['alive']} alive / {summary['total']} total"
            )
            color = "#34d399" if summary["alive"] > 0 else "#f87171"
            self.proxy_indicator.setStyleSheet(
                f"font-size: 10px; color: {color}; "
                "font-family: 'Cascadia Code', 'Consolas', monospace;"
            )
        else:
            self.proxy_indicator.setText(f"{summary['total']} loaded (untested)")
            self.proxy_indicator.setStyleSheet(
                "font-size: 10px; color: #eab308; "
                "font-family: 'Cascadia Code', 'Consolas', monospace;"
            )

    def _get_proxy_summary(self) -> dict:
        if self.main_window:
            proxy_page = self.main_window.get_page("Proxies")
            if proxy_page:
                return proxy_page.get_proxy_summary()
        return {"total": 0, "alive": 0, "tested": False}

    def _get_proxies(self) -> list[str]:
        if not self.proxy_toggle.isChecked():
            return []
        if self.main_window:
            proxy_page = self.main_window.get_page("Proxies")
            if proxy_page:
                return proxy_page.get_proxies()
        return []

    def _go_to_proxy_page(self):
        if self.main_window:
            self.main_window.navigate_to("Proxies")

    # ─── Build jobs ──────────────────────────────────────────────────────

    def _build_jobs(self) -> list[JobConfig]:
        countries = self._get_selected_countries()
        if not countries:
            return []

        selected_cats = self._get_selected_categories()
        # If no categories were previewed, pass empty = all categories
        no_preview = len(self._category_checkboxes) == 0
        all_cats_checked = no_preview or len(selected_cats) == len(self._category_checkboxes)

        threads_text = self.threads_combo.currentText()
        num_jobs = len(countries)
        concurrency = resolve_concurrency(threads_text, num_jobs)
        base_output = self.output_input.text().strip() or "output"

        jobs = []
        for country in countries:
            # Filter categories to only those for this country
            country_cats = []
            if not all_cats_checked and selected_cats:
                # Get slugs from this country's categories
                country_all = {
                    cat.get("slug", ""): cat
                    for cat in self._categories_cache.get(country, [])
                }
                country_cats = [s for s in selected_cats if s in country_all]
                # If all of this country's cats are selected, pass empty (= all)
                if len(country_cats) == len(country_all):
                    country_cats = []

            job = JobConfig(
                country=country,
                categories=country_cats,
                limit=self.limit_spin.value(),
                output_dir=os.path.join(base_output, country),
                concurrency=concurrency,
                fresh=self.fresh_check.isChecked(),
                dry_run=self.dry_run_check.isChecked(),
                quiet=not self.verbose_check.isChecked(),
            )
            jobs.append(job)

        return jobs

    # ─── Start / Stop ────────────────────────────────────────────────────

    def _start_scrape(self):
        countries = self._get_selected_countries()
        if not countries:
            self._log("Select at least one country", "#f87171")
            return

        # Filter out countries already running
        new_countries = [c for c in countries if c not in self._running_countries]
        if not new_countries:
            self._log("Selected countries are already running", "#eab308")
            return

        jobs = self._build_jobs()
        # Only keep jobs for new countries
        jobs = [j for j in jobs if j.country_key in new_countries]
        if not jobs:
            return

        # Ensure output dirs
        for job in jobs:
            job.ensure_dirs()

        proxies = self._get_proxies()
        self._is_running = True
        self.btn_stop.setEnabled(True)
        self.btn_fetch_cats.setEnabled(False)
        self._update_button_styles()

        # Show monitor panel and start dashboard timer
        self._show_monitor()
        if not hasattr(self, '_start_time') or not self._running_countries:
            self._start_time = time.time()
        self._dashboard_timer.start()

        # Track new running countries
        for c in new_countries:
            self._running_countries.add(c)

        # DON'T clear log — append separator
        self._log("═" * 60)
        self._log(
            f"Starting: {', '.join(c.upper() for c in new_countries)} "
            f"({len(jobs)} jobs)",
            "#60a5fa"
        )

        # Add progress bars for new countries (don't clear existing)
        self._add_progress_bars(new_countries)

        # Set limits on new progress bars
        limit = self.limit_spin.value()
        for c in new_countries:
            if c in self._country_progress:
                self._country_progress[c].set_limit(limit)

        # Create and start worker for new countries
        worker = ScrapeWorker(jobs, proxies)
        worker.business_scraped.connect(self._on_business_scraped)
        worker.stats_update.connect(self._on_stats_update)
        worker.log_message.connect(self._on_log_message)
        worker.job_started.connect(self._on_job_started)
        worker.job_completed.connect(self._on_job_completed)
        worker.job_error.connect(self._on_job_error)
        worker.checkpoint_info.connect(self._on_checkpoint_info)
        worker.category_progress.connect(self._on_category_progress)
        worker.pipeline_finished.connect(lambda w=worker: self._on_pipeline_finished(w))
        # New signals
        worker.cf_status.connect(self._on_cf_status)
        worker.categories_found.connect(self._on_categories_streamed)
        worker.cookie_status.connect(self._on_cookie_status)
        worker.phase_changed.connect(self._on_phase_changed)
        self._workers.append(worker)

        # Verbose mode: attach Python logging → GUI log panel (once)
        if self.verbose_check.isChecked() and not self._qt_log_handler:
            import logging as _logging
            self._qt_log_handler = QtLogHandler(
                lambda country, level, msg: self._on_log_message(country, level, msg)
            )
            self._qt_log_handler.setFormatter(_logging.Formatter("%(message)s"))
            _logging.getLogger("arablocal").addHandler(self._qt_log_handler)

        # Update Start button text
        self.btn_start.setText("▶  Add Countries")

        worker.start()

    def _stop_scrape(self):
        for worker in self._workers:
            worker.request_cancel()
        self.btn_stop.setEnabled(False)
        self._log("Stop All requested - finishing in-flight tasks...", "#eab308")

    def _stop_country(self, country: str):
        for worker in self._workers:
            worker.cancel_country(country)
        self._log(f"[{country.upper()}] Stop requested", "#eab308")

    def _add_progress_bars(self, countries: list[str]):
        """Add progress bars for new countries without clearing existing ones."""
        if self.progress_placeholder.parent():
            self.progress_placeholder.setParent(None)

        for country in countries:
            if country not in self._country_progress:
                bar = CountryProgressBar(country)
                bar.country_stop_requested = self._stop_country
                self._country_progress[country] = bar
                self.progress_container.addWidget(bar)

    # ─── Signal handlers ─────────────────────────────────────────────────

    def _on_business_scraped(self, country: str, name: str, url: str):
        short = name[:40] if name else url.split("/")[-1][:30]
        self._log(f"[{country.upper()}] {short}", "#b0b0b6")

    def _on_stats_update(self, country: str, stats: ScrapeStats):
        self._stats_per_country[country] = stats

        if country in self._country_progress:
            self._country_progress[country].update_stats(stats)

        # Update aggregate
        total_s = sum(s.scraped for s in self._stats_per_country.values())
        total_e = sum(s.errors for s in self._stats_per_country.values())
        total_sk = sum(s.skipped for s in self._stats_per_country.values())
        self.lbl_total_scraped.setText(str(total_s))
        self.lbl_total_errors.setText(str(total_e))
        self.lbl_total_skipped.setText(str(total_sk))

    def _on_log_message(self, country: str, level: str, msg: str):
        color_map = {
            "INFO": "#88888f",
            "SUCCESS": "#34d399",
            "WARNING": "#eab308",
            "ERROR": "#f87171",
        }
        color = color_map.get(level, "#88888f")
        self._log(f"[{country.upper()}] {msg}", color)

    def _on_job_started(self, country: str):
        if country in self._country_progress:
            self._country_progress[country].set_started()
        self._log(f"[{country.upper()}] Pipeline started", "#60a5fa")

    def _on_job_completed(self, country: str, stats: ScrapeStats):
        if country in self._country_progress:
            self._country_progress[country].set_completed(stats)
        self._running_countries.discard(country)
        self._log(
            f"[{country.upper()}] Completed: {stats.scraped} businesses "
            f"in {stats.elapsed:.0f}s ({stats.rate:.1f}/min)",
            "#34d399"
        )

    def _on_job_error(self, country: str, error: str):
        if country in self._country_progress:
            self._country_progress[country].set_error(error)
        self._running_countries.discard(country)
        self._log(f"[{country.upper()}] Error: {error}", "#f87171")

    def _on_checkpoint_info(self, country: str, completed: int, total: int):
        self._log(
            f"[{country.upper()}] Checkpoint: {completed}/{total} categories done — resuming",
            "#60a5fa"
        )

    def _on_category_progress(self, country: str, cat_name: str, page_num: int, total_pages: int, new_urls: int):
        if country in self._country_progress:
            self._country_progress[country].update_category_progress(
                cat_name, page_num, total_pages, new_urls
            )

    def _on_cf_status(self, country: str, status: str):
        """Handle CF solving status updates from the worker."""
        self._log(f"[{country.upper()}] {status}", "#60a5fa")
        if country in self._country_progress:
            self._country_progress[country].lbl_status.setText(status[:50])
            self._country_progress[country].lbl_status.setStyleSheet(
                "font-size: 11px; color: #60a5fa;"
            )

    def _on_cookie_status(self, country: str, status: str):
        """Update cookie status indicator in the UI."""
        if status == "alive":
            self.cookie_dot.setStyleSheet("font-size: 10px; color: #34d399;")
            self.cookie_status_label.setText("Active")
            self.cookie_status_label.setStyleSheet(
                "font-size: 10px; color: #34d399; "
                "font-family: 'Cascadia Code', 'Consolas', monospace;"
            )
        elif status == "expired":
            self.cookie_dot.setStyleSheet("font-size: 10px; color: #eab308;")
            self.cookie_status_label.setText("Expired")
            self.cookie_status_label.setStyleSheet(
                "font-size: 10px; color: #eab308; "
                "font-family: 'Cascadia Code', 'Consolas', monospace;"
            )
        else:
            self.cookie_dot.setStyleSheet("font-size: 10px; color: #56565e;")
            self.cookie_status_label.setText("Not needed")
            self.cookie_status_label.setStyleSheet(
                "font-size: 10px; color: #56565e; "
                "font-family: 'Cascadia Code', 'Consolas', monospace;"
            )

    def _on_categories_streamed(self, country: str, cats: list):
        """Auto-populate category preview when Start discovers categories."""
        self._categories_cache[country] = cats
        self._rebuild_category_checkboxes()
        total = sum(len(self._categories_cache.get(c, [])) for c in self._get_selected_countries())
        self.cat_fetch_status.setText(f"{total} categories discovered")
        self.cat_fetch_status.setStyleSheet("font-size: 11px; color: #34d399;")

    def _on_phase_changed(self, country: str, phase: str):
        """Handle pipeline phase transitions."""
        phase_labels = {
            "cf_solve": "Solving CF...",
            "discovery": "Discovering...",
            "scraping": "Scraping...",
        }
        label = phase_labels.get(phase, phase)
        if country in self._country_progress:
            self._country_progress[country].lbl_status.setText(label)
            color = "#60a5fa" if phase != "scraping" else "#eab308"
            self._country_progress[country].lbl_status.setStyleSheet(
                f"font-size: 11px; color: {color};"
            )

    def _on_pipeline_finished(self, finished_worker=None):
        # Remove finished worker from list
        if finished_worker and finished_worker in self._workers:
            self._workers.remove(finished_worker)

        # Check if ALL workers are done
        if self._workers:
            # Some workers still running
            self._log("Worker batch finished. Other countries still running.", "#60a5fa")
            return

        # All done
        self._is_running = False
        self._running_countries.clear()
        self.btn_start.setText("▶  Start Scraping")
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.btn_fetch_cats.setEnabled(len(self._get_selected_countries()) > 0)
        self._update_button_styles()
        self._dashboard_timer.stop()
        self._log("All pipelines finished.", "#34d399")

        # Remove verbose log handler if attached
        if self._qt_log_handler:
            import logging as _logging
            _logging.getLogger("arablocal").removeHandler(self._qt_log_handler)
            self._qt_log_handler = None

        # Final dashboard refresh + clear running card
        if self.main_window:
            dashboard = self.main_window.get_page("Dashboard")
            if dashboard:
                dashboard.refresh_stats()
                dashboard.clear_running_status()

    # ─── Logging ─────────────────────────────────────────────────────────

    def _log(self, msg: str, color: str = "#b0b0b6"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_output.append(
            f'<span style="color:#56565e">{timestamp}</span> '
            f'<span style="color:{color}">{msg}</span>'
        )
        sb = self.log_output.verticalScrollBar()
        sb.setValue(sb.maximum())

    # ─── Button state styling ────────────────────────────────────────────

    def _update_button_styles(self):
        if self._is_running:
            self.btn_start.setStyleSheet(
                "QPushButton { background: #0a2e22; color: #34d399; "
                "border: 1px solid #166d4e; border-radius: 6px; "
                "padding: 7px 16px; font-weight: 600; font-size: 12px; }"
                "QPushButton:hover { background: #166d4e; color: #5eead4; }"
            )
            self.btn_stop.setStyleSheet(
                "QPushButton { background: #991b1b; color: #f87171; "
                "border: 1px solid #7f1d1d; border-radius: 6px; "
                "padding: 7px 16px; font-weight: 600; font-size: 12px; }"
                "QPushButton:hover { background: #b91c1c; }"
            )
        else:
            self.btn_start.setStyleSheet(
                "QPushButton { background: #34d399; color: #0c0c0e; "
                "border: none; border-radius: 6px; "
                "padding: 7px 16px; font-weight: 600; font-size: 12px; }"
                "QPushButton:hover { background: #5eead4; }"
            )
            self.btn_stop.setStyleSheet(
                "QPushButton { background: #1a1a1f; color: #35353b; "
                "border: 1px solid #252529; border-radius: 6px; "
                "padding: 7px 16px; font-weight: 500; font-size: 12px; }"
            )

    # ─── External API ────────────────────────────────────────────────────

    def showEvent(self, event):
        super().showEvent(event)
        self._update_proxy_indicator()
        self._update_button_styles()

