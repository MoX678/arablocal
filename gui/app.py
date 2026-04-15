"""ArabLocal Scraper GUI — application entry point.

Usage:
    python -m gui.app
    or via PyInstaller exe
"""

import sys
import os

# PyInstaller freeze support: resolve base path
if getattr(sys, "frozen", False):
    # Running as bundled EXE — _MEIPASS is the temp extraction dir
    _BASE_DIR = sys._MEIPASS
    # Also add the EXE's directory for output/config files
    _APP_DIR = os.path.dirname(sys.executable)
    os.chdir(_APP_DIR)
else:
    _BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Ensure the project root is on the path
sys.path.insert(0, _BASE_DIR)


def main():
    # Step 1: Check dependencies (packages + browser install)
    from gui.bootstrap import ensure_dependencies
    if not ensure_dependencies():
        # Installer handled restart or user closed the dialog
        sys.exit(0)

    # Suppress terminal output — all logging goes through GUI only
    import logging
    _logger = logging.getLogger("arablocal")
    _logger.addHandler(logging.NullHandler())
    _logger.propagate = False
    _logger.setLevel(logging.DEBUG)

    # Also silence scrapling's internal logger (prints "Fetched (200)..." to terminal)
    logging.getLogger("scrapling").setLevel(logging.CRITICAL)
    logging.getLogger("scrapling").propagate = False

    # Step 2: Launch Qt application
    from PyQt6.QtWidgets import QApplication
    from PyQt6.QtCore import Qt

    # High-DPI support
    os.environ.setdefault("QT_ENABLE_HIGHDPI_SCALING", "1")

    app = QApplication(sys.argv)
    app.setApplicationName("ArabLocal Scraper")
    app.setApplicationVersion("3.0")

    # Apply dark theme
    from gui.theme import STYLESHEET
    app.setStyleSheet(STYLESHEET)

    # Create and show main window
    from gui.main_window import MainWindow

    # Catch unhandled exceptions so the app doesn't silently crash
    def _exception_hook(exc_type, exc_value, exc_tb):
        import traceback
        traceback.print_exception(exc_type, exc_value, exc_tb)
        sys.__excepthook__(exc_type, exc_value, exc_tb)

    sys.excepthook = _exception_hook

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        input("Press Enter to exit...")
