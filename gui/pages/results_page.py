"""Results browser page — view scraped data from SQLite with filters and export."""

from __future__ import annotations

import csv
import json
import os
import sqlite3
from pathlib import Path

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QComboBox, QLineEdit, QTableWidget, QTableWidgetItem,
    QHeaderView, QFileDialog, QMessageBox,
)

from core.config import BASE_URLS, COUNTRY_INFO, BASE_FIELDS

# Columns to show in the table (order matters)
_DISPLAY_COLS = ["Country", "Category"] + BASE_FIELDS + ["URL", "Scraped"]


class ResultsPage(QWidget):
    def __init__(self, main_window=None, parent=None):
        super().__init__(parent)
        self.main_window = main_window
        self._current_data: list[dict] = []
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(8)

        # Header row — section label + count inline
        header_row = QHBoxLayout()
        header_row.setSpacing(12)

        header = QLabel("RESULTS")
        header.setStyleSheet(
            "font-size: 10px; font-weight: 700; color: #56565e; "
            "letter-spacing: 2px;"
        )
        header_row.addWidget(header)

        self.count_label = QLabel("")
        self.count_label.setStyleSheet(
            "font-size: 10px; color: #35353b; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        header_row.addWidget(self.count_label)
        header_row.addStretch()
        layout.addLayout(header_row)

        # Filters row
        filter_row = QHBoxLayout()
        filter_row.setSpacing(8)

        lbl_country = QLabel("COUNTRY")
        lbl_country.setStyleSheet(
            "font-size: 9px; font-weight: 700; color: #35353b; letter-spacing: 1px;"
        )
        filter_row.addWidget(lbl_country)

        self.country_combo = QComboBox()
        self.country_combo.addItem("All Countries", "all")
        for key, info in COUNTRY_INFO.items():
            self.country_combo.addItem(f"{info['name']} ({key.upper()})", key)
        self.country_combo.currentIndexChanged.connect(self._load_data)
        filter_row.addWidget(self.country_combo)

        lbl_cat = QLabel("CATEGORY")
        lbl_cat.setStyleSheet(
            "font-size: 9px; font-weight: 700; color: #35353b; letter-spacing: 1px;"
        )
        filter_row.addWidget(lbl_cat)

        self.category_combo = QComboBox()
        self.category_combo.addItem("All Categories", "all")
        self.category_combo.currentIndexChanged.connect(self._apply_filter)
        filter_row.addWidget(self.category_combo)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search...")
        self.search_input.textChanged.connect(self._apply_filter)
        filter_row.addWidget(self.search_input, stretch=1)

        btn_refresh = QPushButton("Refresh")
        btn_refresh.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_refresh.clicked.connect(self._load_data)
        filter_row.addWidget(btn_refresh)

        layout.addLayout(filter_row)

        # Data table
        self.table = QTableWidget()
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        self.table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.Interactive
        )
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.verticalHeader().setDefaultSectionSize(30)
        layout.addWidget(self.table, stretch=1)

        # Export row
        btn_row = QHBoxLayout()
        btn_row.setSpacing(6)

        btn_export = QPushButton("Export CSV")
        btn_export.setProperty("class", "primary")
        btn_export.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_export.clicked.connect(self._export_csv)
        btn_row.addWidget(btn_export)

        btn_open = QPushButton("Open Folder")
        btn_open.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_open.clicked.connect(self._open_output_folder)
        btn_row.addWidget(btn_open)

        btn_row.addStretch()
        layout.addLayout(btn_row)

    # ─── Data loading ────────────────────────────────────────────────────

    def showEvent(self, event):
        super().showEvent(event)
        self._load_data()

    def _load_data(self):
        """Load data from per-country SQLite databases, parsing JSON data column."""
        country_filter = self.country_combo.currentData()
        countries = list(BASE_URLS.keys()) if country_filter == "all" else [country_filter]

        all_rows: list[dict] = []
        categories_seen: set[str] = set()

        for key in countries:
            db_path = Path("output") / key / f"{key}_staging.db"
            if not db_path.exists():
                continue
            try:
                conn = sqlite3.connect(str(db_path))
                cur = conn.cursor()
                cur.execute("SELECT url, category, data, scraped_at FROM businesses ORDER BY rowid DESC")
                for url, category, data_json, scraped_at in cur.fetchall():
                    # Parse the JSON data column into flat fields
                    try:
                        fields = json.loads(data_json) if data_json else {}
                    except (json.JSONDecodeError, TypeError):
                        fields = {}

                    row = {
                        "Country": key.upper(),
                        "Category": category or "",
                        "URL": url or "",
                        "Scraped": (scraped_at or "")[:19],
                    }
                    # Merge parsed fields (Name, Phone_1, Email, etc.)
                    for f in BASE_FIELDS:
                        row[f] = fields.get(f, "")

                    categories_seen.add(category or "")
                    all_rows.append(row)
                conn.close()
            except Exception:
                pass

        self._current_data = all_rows

        # Rebuild category filter
        prev = self.category_combo.currentData()
        self.category_combo.blockSignals(True)
        self.category_combo.clear()
        self.category_combo.addItem("All Categories", "all")
        for cat in sorted(categories_seen):
            if cat:
                self.category_combo.addItem(cat.replace("_", " ").title(), cat)
        # Restore previous selection if still valid
        idx = self.category_combo.findData(prev)
        self.category_combo.setCurrentIndex(max(0, idx))
        self.category_combo.blockSignals(False)

        self._apply_filter()

    def _apply_filter(self):
        """Filter rows and display in the table."""
        search = self.search_input.text().strip().lower()
        cat_filter = self.category_combo.currentData()
        filtered = self._current_data

        if cat_filter and cat_filter != "all":
            filtered = [r for r in filtered if r.get("Category") == cat_filter]

        if search:
            filtered = [
                r for r in filtered
                if any(search in str(v).lower() for v in r.values())
            ]

        # Figure out which columns actually have data
        cols_with_data = []
        for col in _DISPLAY_COLS:
            if any(r.get(col) for r in filtered):
                cols_with_data.append(col)
        if not cols_with_data:
            cols_with_data = ["Country", "Category", "Name", "URL"]

        self.table.setSortingEnabled(False)
        self.table.setRowCount(len(filtered))
        self.table.setColumnCount(len(cols_with_data))
        self.table.setHorizontalHeaderLabels(cols_with_data)

        for row_idx, row_data in enumerate(filtered):
            for col_idx, col_name in enumerate(cols_with_data):
                value = str(row_data.get(col_name, ""))
                item = QTableWidgetItem(value)
                self.table.setItem(row_idx, col_idx, item)

        self.table.setSortingEnabled(True)
        self.count_label.setText(
            f"{len(filtered):,} shown  /  {len(self._current_data):,} total"
        )

    # ─── Export ──────────────────────────────────────────────────────────

    def _export_csv(self):
        if not self._current_data:
            QMessageBox.information(self, "Export", "No data to export.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self, "Export CSV", "export.csv", "CSV Files (*.csv)"
        )
        if not path:
            return

        cols = [c for c in _DISPLAY_COLS if any(r.get(c) for r in self._current_data)]
        if not cols:
            cols = _DISPLAY_COLS

        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(self._current_data)
            QMessageBox.information(
                self, "Export", f"Exported {len(self._current_data):,} records to {path}"
            )
        except Exception as e:
            QMessageBox.critical(self, "Export Error", str(e))

    def _open_output_folder(self):
        output_dir = os.path.abspath("output")
        if os.path.isdir(output_dir):
            os.startfile(output_dir)
        else:
            QMessageBox.information(self, "Output", "Output directory does not exist yet.")
