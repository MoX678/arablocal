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
    # Tell patchright/playwright to use the system browser cache
    # (bundled exe defaults to .local-browsers which is empty)
    _pw_dir = os.path.join(os.environ.get("LOCALAPPDATA", ""), "ms-playwright")
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", _pw_dir)
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

    # ── Error / crash log folder ─────────────────────────────────────────
    _errors_dir = os.path.join(os.getcwd(), "errors")
    os.makedirs(_errors_dir, exist_ok=True)

    _err_handler = logging.FileHandler(
        os.path.join(_errors_dir, "error.log"), encoding="utf-8"
    )
    _err_handler.setLevel(logging.ERROR)
    _err_handler.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    _logger.addHandler(_err_handler)

    # Full debug log — captures everything (discovery, fetch, CF) for troubleshooting
    _debug_handler = logging.FileHandler(
        os.path.join(_errors_dir, "debug.log"), encoding="utf-8", mode="w",
    )
    _debug_handler.setLevel(logging.DEBUG)
    _debug_handler.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S"
    ))
    _logger.addHandler(_debug_handler)

    # Also silence scrapling's internal logger (prints "Fetched (200)..." to terminal)
    logging.getLogger("scrapling").setLevel(logging.CRITICAL)
    logging.getLogger("scrapling").propagate = False

    # Step 2: Launch Qt application
    from PyQt6.QtWidgets import QApplication
    from PyQt6.QtCore import Qt

    # High-DPI support
    os.environ.setdefault("QT_ENABLE_HIGHDPI_SCALING", "1")

    app = QApplication(sys.argv)
    from core import __version__
    app.setApplicationName("ArabLocal Scraper")
    app.setApplicationVersion(__version__)

    # Apply dark theme
    from gui.theme import STYLESHEET
    app.setStyleSheet(STYLESHEET)

    # Create and show main window
    from gui.main_window import MainWindow

    # Catch unhandled exceptions so the app doesn't silently crash
    def _exception_hook(exc_type, exc_value, exc_tb):
        import traceback
        from datetime import datetime
        tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        _logger.error(tb_str)
        # Write individual crash file
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            crash_path = os.path.join(_errors_dir, f"crash_{ts}.log")
            with open(crash_path, "w", encoding="utf-8") as f:
                f.write(f"ArabLocal Scraper — Unhandled Exception\n")
                f.write(f"Time: {datetime.now().isoformat()}\n")
                f.write(f"{'=' * 60}\n\n")
                f.write(tb_str)
        except Exception:
            pass
        sys.__excepthook__(exc_type, exc_value, exc_tb)

    sys.excepthook = _exception_hook

    window = MainWindow()
    window.show()

    # Start background update check
    window.start_update_check()

    sys.exit(app.exec())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
