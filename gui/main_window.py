"""Main application window with sidebar navigation."""

import sys
from PyQt6.QtCore import Qt, QSize
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QStackedWidget, QLabel, QStatusBar, QFrame, QProgressBar,
    QMessageBox, QDialog,
)

from gui.pages.dashboard_page import DashboardPage
from gui.pages.proxy_page import ProxyPage
from gui.pages.scrape_page import ScrapePage
from gui.pages.results_page import ResultsPage


class MainWindow(QMainWindow):
    """Single main window with sidebar navigation and stacked pages."""

    def __init__(self):
        super().__init__()
        from core import __version__
        self.setWindowTitle(f"ArabLocal Scraper v{__version__}")
        self.setMinimumSize(1100, 700)
        self.resize(1280, 800)

        # Central widget
        central = QWidget()
        self.setCentralWidget(central)
        layout = QHBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # -- Sidebar --
        self.sidebar = QFrame()
        self.sidebar.setObjectName("sidebar")
        sidebar_layout = QVBoxLayout(self.sidebar)
        sidebar_layout.setContentsMargins(0, 12, 0, 12)
        sidebar_layout.setSpacing(0)

        # App title in sidebar
        title = QLabel("ARABLOCAL")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title.setStyleSheet(
            "font-size: 10px; font-weight: 700; color: #56565e; "
            "letter-spacing: 2px; padding: 8px 0; margin-bottom: 8px; "
            "border-bottom: 1px solid #252529;"
        )
        sidebar_layout.addWidget(title)

        # Navigation buttons
        self.nav_buttons: list[QPushButton] = []
        pages_info = [
            ("Dashboard", "System overview"),
            ("Scrape", "Configure & run"),
            ("Proxies", "Test & manage"),
            ("Results", "Browse data"),
        ]

        for label, tooltip in pages_info:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setToolTip(tooltip)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.clicked.connect(lambda checked, l=label: self._on_nav_click(l))
            sidebar_layout.addWidget(btn)
            self.nav_buttons.append(btn)

        sidebar_layout.addStretch()

        # Version label at bottom
        version_label = QLabel(f"v{__version__}")
        version_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        version_label.setStyleSheet(
            "color: #35353b; font-size: 10px; "
            "font-family: 'Cascadia Code', 'Consolas', monospace;"
        )
        sidebar_layout.addWidget(version_label)

        layout.addWidget(self.sidebar)

        # -- Page stack --
        self.stack = QStackedWidget()
        layout.addWidget(self.stack)

        # Create pages
        self.pages: dict[str, QWidget] = {}
        self._create_pages()

        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")

        # ── Update banner (hidden by default, themed via design tokens) ───
        from gui.theme import COLORS
        self._update_info: dict | None = None
        self._update_state: str = "idle"  # idle|available|downloading|applying|failed
        self._update_banner = QFrame()
        self._update_banner.setObjectName("updateBanner")
        self._update_banner.setStyleSheet(
            "#updateBanner {"
            f"  background: {COLORS['accent_muted']};"
            f"  border: 1px solid {COLORS['accent_dim']};"
            "  border-radius: 6px; margin: 3px 6px;"
            "}"
        )
        self._update_banner.setVisible(False)
        ub_layout = QHBoxLayout(self._update_banner)
        ub_layout.setContentsMargins(10, 4, 6, 4)
        ub_layout.setSpacing(8)

        # Leading status dot — calm emerald, becomes amber/red on failure
        self._update_dot = QLabel("\u25CF")
        self._update_dot.setStyleSheet(
            f"color: {COLORS['accent']}; font-size: 9px; "
            "background: transparent; border: none;"
        )
        self._update_dot.setFixedWidth(10)
        ub_layout.addWidget(self._update_dot)

        self._update_label = QLabel()
        self._update_label.setStyleSheet(
            f"color: {COLORS['accent']}; font-size: 11px; font-weight: 600; "
            "background: transparent; border: none;"
        )
        ub_layout.addWidget(self._update_label)

        self._update_progress = QProgressBar()
        self._update_progress.setFixedWidth(140)
        self._update_progress.setFixedHeight(6)
        self._update_progress.setTextVisible(False)
        self._update_progress.setVisible(False)
        # Inherits app-level QProgressBar style (thin emerald instrument strip)
        ub_layout.addWidget(self._update_progress)

        self._update_btn = QPushButton("Details")
        self._update_btn.setProperty("class", "primary")
        self._update_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._update_btn.setFixedHeight(22)
        self._update_btn.setStyleSheet(
            "QPushButton[class=\"primary\"] {"
            f"  background-color: {COLORS['accent']};"
            f"  color: {COLORS['base']};"
            "  font-size: 11px; font-weight: 700;"
            "  padding: 2px 12px; border-radius: 4px; border: none;"
            "}"
            "QPushButton[class=\"primary\"]:hover {"
            f"  background-color: {COLORS['accent_hover']}; }}"
            "QPushButton[class=\"primary\"]:disabled {"
            f"  background-color: {COLORS['surface1']};"
            f"  color: {COLORS['overlay0']}; }}"
        )
        self._update_btn.clicked.connect(self._on_update_click)
        ub_layout.addWidget(self._update_btn)

        self._update_dismiss = QPushButton("\u2715")
        self._update_dismiss.setCursor(Qt.CursorShape.PointingHandCursor)
        self._update_dismiss.setToolTip("Dismiss for now")
        self._update_dismiss.setFixedSize(20, 22)
        self._update_dismiss.setStyleSheet(
            "QPushButton {"
            f"  background: transparent; color: {COLORS['overlay0']};"
            "  border: none; font-size: 12px; padding: 0;"
            "}"
            "QPushButton:hover {"
            f"  color: {COLORS['text']}; }}"
        )
        self._update_dismiss.clicked.connect(
            lambda: self._update_banner.setVisible(False)
        )
        ub_layout.addWidget(self._update_dismiss)

        self.status_bar.addPermanentWidget(self._update_banner)

        # Select Dashboard by default
        self._on_nav_click("Dashboard")

    def _create_pages(self):
        """Instantiate all page widgets and add to stack."""
        page_classes = {
            "Dashboard": DashboardPage,
            "Scrape": ScrapePage,
            "Proxies": ProxyPage,
            "Results": ResultsPage,
        }
        for name, cls in page_classes.items():
            page = cls(main_window=self)
            self.pages[name] = page
            self.stack.addWidget(page)

    def _on_nav_click(self, label: str):
        """Handle sidebar navigation click."""
        # Update button states
        for btn in self.nav_buttons:
            btn.setChecked(btn.text() == label)

        # Switch page
        if label in self.pages:
            self.stack.setCurrentWidget(self.pages[label])

    def get_page(self, name: str) -> QWidget:
        """Get a page by name for cross-page communication."""
        return self.pages.get(name)

    def navigate_to(self, page_name: str):
        """Navigate to a page by name (for cross-page CTAs)."""
        if page_name in self.pages:
            self._on_nav_click(page_name)

    # ─── Auto-update ─────────────────────────────────────────────────────

    def start_update_check(self):
        """Start background update check (called from app.py after show)."""
        from core import __version__
        from gui.workers import UpdateCheckWorker
        self._update_worker = UpdateCheckWorker(__version__, parent=self)
        self._update_worker.update_available.connect(self._on_update_available)
        self._update_worker.start()

    # ─── State helpers ──────────────────────────────────────────────────

    def _set_banner_state(self, state: str, label: str):
        """Update banner visuals based on state. Keeps colors token-aligned."""
        from gui.theme import COLORS
        self._update_state = state
        self._update_label.setText(label)

        if state == "failed":
            color = COLORS["red"]
        elif state == "applying":
            color = COLORS["yellow"]
        else:
            color = COLORS["accent"]

        self._update_dot.setStyleSheet(
            f"color: {color}; font-size: 9px; "
            "background: transparent; border: none;"
        )
        self._update_label.setStyleSheet(
            f"color: {color}; font-size: 11px; font-weight: 600; "
            "background: transparent; border: none;"
        )

    def _on_update_available(self, info: dict):
        """Show update banner when a newer version is found."""
        self._update_info = info
        version = info.get("version", "?")
        size_mb = info.get("asset_size", 0) / (1024 * 1024)
        self._set_banner_state(
            "available", f"v{version} available  ·  {size_mb:.1f} MB"
        )
        self._update_btn.setText("Details")
        self._update_btn.setEnabled(True)
        self._update_progress.setVisible(False)
        self._update_dismiss.setVisible(True)
        self._update_banner.setVisible(True)
        self.status_bar.showMessage(f"Update available: v{version}")

    def _on_update_click(self):
        """Open the themed update dialog and route the user's choice."""
        if not self._update_info:
            return

        # If a download is already in progress, the button is disabled — guard
        if self._update_state in ("downloading", "applying"):
            return

        from core import __version__
        from gui.update_dialog import UpdateDialog
        from core.updater import set_skipped_version

        dlg = UpdateDialog(self._update_info, __version__, parent=self)
        result = dlg.exec()

        if result == QDialog.DialogCode.Accepted:
            self._start_download()
        elif result == UpdateDialog.SKIP:
            tag = self._update_info.get("version", "")
            if tag:
                set_skipped_version(tag)
            self._update_banner.setVisible(False)
        else:
            # Remind me later — keep banner visible for this session
            pass

    def _start_download(self):
        from gui.workers import UpdateDownloadWorker

        self._set_banner_state("downloading", "Downloading…")
        self._update_btn.setEnabled(False)
        self._update_btn.setText("…")
        self._update_dismiss.setVisible(False)
        self._update_progress.setVisible(True)
        self._update_progress.setValue(0)

        self._dl_worker = UpdateDownloadWorker(
            self._update_info["download_url"], parent=self
        )
        self._dl_worker.progress.connect(self._on_download_progress)
        self._dl_worker.finished.connect(self._on_download_finished)
        self._dl_worker.start()

    def _on_download_progress(self, downloaded: int, total: int):
        if total > 0:
            pct = int(downloaded * 100 / total)
            self._update_progress.setValue(pct)
            done_mb = downloaded / (1024 * 1024)
            total_mb = total / (1024 * 1024)
            self._set_banner_state(
                "downloading",
                f"Downloading  {done_mb:.1f} / {total_mb:.1f} MB",
            )

    def _on_download_finished(self, zip_path: str):
        if not zip_path:
            self._set_banner_state("failed", "Download failed — check connection")
            self._update_btn.setEnabled(True)
            self._update_btn.setText("Retry")
            self._update_dismiss.setVisible(True)
            self._update_progress.setVisible(False)
            return

        self._set_banner_state("applying", "Applying update…")
        self._update_progress.setValue(100)

        from core.updater import apply_update
        if apply_update(zip_path):
            self._set_banner_state("applying", "Restarting…")
            # Close the app — the batch script will relaunch
            sys.exit(0)
        else:
            self._set_banner_state("failed", "Update failed — try manual install")
            self._update_btn.setEnabled(True)
            self._update_btn.setText("Retry")
            self._update_dismiss.setVisible(True)
