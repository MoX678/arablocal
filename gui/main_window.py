"""Main application window with sidebar navigation."""

import sys
from PyQt6.QtCore import Qt, QSize
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QStackedWidget, QLabel, QStatusBar, QFrame, QProgressBar,
    QMessageBox,
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

        # ── Update banner (hidden by default) ─────────────────────────
        self._update_info: dict | None = None
        self._update_banner = QFrame()
        self._update_banner.setObjectName("updateBanner")
        self._update_banner.setStyleSheet(
            "#updateBanner {"
            "  background: #1a2e23; border: 1px solid #34d399;"
            "  border-radius: 4px; margin: 4px 8px;"
            "}"
        )
        self._update_banner.setVisible(False)
        ub_layout = QHBoxLayout(self._update_banner)
        ub_layout.setContentsMargins(12, 6, 12, 6)

        self._update_label = QLabel()
        self._update_label.setStyleSheet(
            "color: #34d399; font-size: 12px; font-weight: 600;"
        )
        ub_layout.addWidget(self._update_label)
        ub_layout.addStretch()

        self._update_progress = QProgressBar()
        self._update_progress.setFixedWidth(160)
        self._update_progress.setFixedHeight(14)
        self._update_progress.setVisible(False)
        self._update_progress.setStyleSheet(
            "QProgressBar { background: #18181b; border: 1px solid #27272a; border-radius: 3px; }"
            "QProgressBar::chunk { background: #34d399; border-radius: 2px; }"
        )
        ub_layout.addWidget(self._update_progress)

        self._update_btn = QPushButton("Update Now")
        self._update_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._update_btn.setStyleSheet(
            "QPushButton {"
            "  background: #34d399; color: #0c0c0e; font-weight: 700;"
            "  font-size: 11px; padding: 4px 14px; border-radius: 4px;"
            "}"
            "QPushButton:hover { background: #5eead4; }"
        )
        self._update_btn.clicked.connect(self._on_update_click)
        ub_layout.addWidget(self._update_btn)

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

    def _on_update_available(self, info: dict):
        """Show update banner when a newer version is found."""
        self._update_info = info
        version = info.get("version", "?")
        size_mb = info.get("asset_size", 0) / (1024 * 1024)
        self._update_label.setText(
            f"  v{version} available ({size_mb:.0f} MB)"
        )
        self._update_banner.setVisible(True)
        self.status_bar.showMessage(f"Update available: v{version}")

    def _on_update_click(self):
        """Download and apply update."""
        if not self._update_info:
            return

        reply = QMessageBox.question(
            self,
            "Update ArabLocal Scraper",
            f"Download and install v{self._update_info['version']}?\n\n"
            "The app will restart after the update.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        self._update_btn.setEnabled(False)
        self._update_btn.setText("Downloading...")
        self._update_progress.setVisible(True)
        self._update_progress.setValue(0)

        from gui.workers import UpdateDownloadWorker
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
            self._update_label.setText(
                f"  Downloading... {downloaded // (1024*1024)}/{total // (1024*1024)} MB"
            )

    def _on_download_finished(self, zip_path: str):
        if not zip_path:
            self._update_btn.setEnabled(True)
            self._update_btn.setText("Retry")
            self._update_progress.setVisible(False)
            self._update_label.setText("  Download failed — check connection")
            return

        self._update_label.setText("  Applying update...")
        self._update_progress.setValue(100)

        from core.updater import apply_update
        if apply_update(zip_path):
            self._update_label.setText("  Restarting...")
            # Close the app — the batch script will relaunch
            sys.exit(0)
        else:
            self._update_btn.setEnabled(True)
            self._update_btn.setText("Retry")
            self._update_label.setText("  Update failed — try manual install")
