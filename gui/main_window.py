"""Main application window with sidebar navigation."""

from PyQt6.QtCore import Qt, QSize
from PyQt6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QStackedWidget, QLabel, QStatusBar, QFrame,
)

from gui.pages.dashboard_page import DashboardPage
from gui.pages.proxy_page import ProxyPage
from gui.pages.scrape_page import ScrapePage
from gui.pages.results_page import ResultsPage


class MainWindow(QMainWindow):
    """Single main window with sidebar navigation and stacked pages."""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("ArabLocal Scraper v3")
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
        version_label = QLabel("v3.0")
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
