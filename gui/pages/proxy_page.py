"""Proxy management page — add, load, test proxies."""

from __future__ import annotations

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTextEdit, QTableWidget, QTableWidgetItem,
    QFileDialog, QProgressBar, QHeaderView,
)

from gui.workers import ProxyTestWorker


class ProxyPage(QWidget):
    def __init__(self, main_window=None, parent=None):
        super().__init__(parent)
        self.main_window = main_window
        self._test_worker: ProxyTestWorker | None = None
        self._alive_proxies: list[str] = []
        self._test_completed = False
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(8)

        header = QLabel("PROXIES")
        header.setStyleSheet(
            "font-size: 10px; font-weight: 700; color: #56565e; "
            "letter-spacing: 2px; padding-bottom: 4px; "
            "border-bottom: 1px solid #252529;"
        )
        layout.addWidget(header)

        # ── INPUT section — full width ────────────────────────────────
        input_label = QLabel("INPUT")
        input_label.setStyleSheet(
            "font-size: 9px; font-weight: 700; color: #35353b; letter-spacing: 1.5px;"
        )
        layout.addWidget(input_label)

        self.proxy_input = QTextEdit()
        self.proxy_input.setPlaceholderText(
            "http://user:pass@host:port  •  socks5://host:port  •  host:port"
        )
        self.proxy_input.setMaximumHeight(140)
        layout.addWidget(self.proxy_input)

        # ── Action bar — compact single row ───────────────────────────
        action_row = QHBoxLayout()
        action_row.setSpacing(6)

        btn_load = QPushButton("Load File")
        btn_load.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_load.clicked.connect(self._load_file)
        action_row.addWidget(btn_load)

        btn_clear = QPushButton("Clear")
        btn_clear.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_clear.clicked.connect(self._clear)
        action_row.addWidget(btn_clear)

        self.btn_test = QPushButton("▶  Test All")
        self.btn_test.setProperty("class", "primary")
        self.btn_test.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_test.clicked.connect(self._test_proxies)
        action_row.addWidget(self.btn_test)

        self.test_progress = QProgressBar()
        self.test_progress.setVisible(False)
        self.test_progress.setFixedHeight(4)
        action_row.addWidget(self.test_progress, 1)

        self.status_label = QLabel("")
        self.status_label.setStyleSheet("font-size: 11px; color: #88888f;")
        action_row.addWidget(self.status_label)

        action_row.addStretch()
        layout.addLayout(action_row)

        # ── RESULTS section — full width table ────────────────────────
        results_row = QHBoxLayout()
        results_row.setSpacing(8)

        results_label = QLabel("RESULTS")
        results_label.setStyleSheet(
            "font-size: 9px; font-weight: 700; color: #35353b; letter-spacing: 1.5px;"
        )
        results_row.addWidget(results_label)

        self.summary_label = QLabel("")
        self.summary_label.setStyleSheet(
            "font-size: 10px; color: #56565e; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        results_row.addWidget(self.summary_label)
        results_row.addStretch()
        layout.addLayout(results_row)

        self.results_table = QTableWidget()
        self.results_table.setColumnCount(3)
        self.results_table.setHorizontalHeaderLabels(["Proxy", "Status", "Latency"])
        self.results_table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeMode.Stretch
        )
        self.results_table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeMode.ResizeToContents
        )
        self.results_table.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.ResizeMode.ResizeToContents
        )
        self.results_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        layout.addWidget(self.results_table, stretch=1)

    def _parse_proxies(self) -> list[str]:
        """Parse proxy list from text area."""
        lines = self.proxy_input.toPlainText().strip().splitlines()
        proxies = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith("#"):
                if not line.startswith(("http://", "https://", "socks")):
                    line = f"http://{line}"
                proxies.append(line)
        return proxies

    def _load_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Load Proxy File", "", "Text Files (*.txt);;All Files (*)"
        )
        if path:
            try:
                with open(path, "r") as f:
                    content = f.read()
                self.proxy_input.setPlainText(content)
                self.status_label.setText(f"Loaded {path}")
                self.status_label.setStyleSheet("font-size: 11px; color: #34d399;")
            except Exception as e:
                self.status_label.setText(f"Error: {e}")
                self.status_label.setStyleSheet("font-size: 11px; color: #f87171;")

    def _clear(self):
        self.proxy_input.clear()
        self.results_table.setRowCount(0)
        self.summary_label.setText("")
        self.status_label.setText("")

    def _test_proxies(self):
        proxies = self._parse_proxies()
        if not proxies:
            self.status_label.setText("No proxies to test")
            self.status_label.setStyleSheet("font-size: 11px; color: #eab308;")
            return

        self._alive_proxies.clear()
        self._test_completed = False
        self.results_table.setRowCount(0)
        self.btn_test.setEnabled(False)
        self.test_progress.setVisible(True)
        self.test_progress.setRange(0, len(proxies))
        self.test_progress.setValue(0)
        self.status_label.setText(f"Testing {len(proxies)} proxies...")
        self.status_label.setStyleSheet("font-size: 11px; color: #88888f;")

        self._test_worker = ProxyTestWorker(proxies)
        self._test_worker.proxy_result.connect(self._on_proxy_result)
        self._test_worker.progress.connect(self._on_test_progress)
        self._test_worker.finished_signal.connect(self._on_test_done)
        self._test_worker.start()

    def _on_proxy_result(self, proxy: str, alive: bool, latency: float):
        if alive:
            self._alive_proxies.append(proxy)

        row = self.results_table.rowCount()
        self.results_table.insertRow(row)

        self.results_table.setItem(row, 0, QTableWidgetItem(proxy))

        status_item = QTableWidgetItem("ALIVE" if alive else "DEAD")
        status_item.setForeground(
            Qt.GlobalColor.green if alive else Qt.GlobalColor.red
        )
        self.results_table.setItem(row, 1, status_item)

        latency_item = QTableWidgetItem(f"{latency:.0f}ms")
        self.results_table.setItem(row, 2, latency_item)

    def _on_test_progress(self, current: int, total: int):
        self.test_progress.setValue(current)
        self.status_label.setText(f"Testing {current}/{total}...")

    def _on_test_done(self, alive: int, total: int):
        self._test_completed = True
        self.btn_test.setEnabled(True)
        self.test_progress.setVisible(False)
        color = "#34d399" if alive > 0 else "#f87171"
        self.summary_label.setText(f"{alive}/{total} proxies alive")
        self.summary_label.setStyleSheet(f"font-size: 12px; color: {color};")
        self.status_label.setText("Test complete")
        self.status_label.setStyleSheet("font-size: 11px; color: #34d399;")

    def get_proxies(self) -> list[str]:
        """Return the current proxy list for use by other pages.
        If proxies were tested, returns only alive ones."""
        if self._test_completed and self._alive_proxies:
            return list(self._alive_proxies)
        return self._parse_proxies()

    def get_alive_proxies(self) -> list[str]:
        """Return only tested-alive proxies."""
        return list(self._alive_proxies)

    def get_proxy_summary(self) -> dict:
        """Return proxy status summary for cross-page display."""
        all_proxies = self._parse_proxies()
        return {
            "total": len(all_proxies),
            "alive": len(self._alive_proxies),
            "tested": self._test_completed,
        }
