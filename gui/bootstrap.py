"""Dependency bootstrap -- checks required packages and installs missing ones.

Uses tkinter (stdlib) for the installer UI since PyQt6 may not be installed yet.
When running as a bundled EXE, Python packages are already bundled --
only the browser binary (chromium) needs to be installed on first run.
"""

import importlib
import os
import subprocess
import sys

REQUIRED_PACKAGES = {
    # import_name: pip_name
    "PyQt6": "PyQt6",
    "scrapling": "scrapling[fetchers]",
    "rich": "rich",
}


def _is_frozen() -> bool:
    """Check if running as a PyInstaller bundle."""
    return getattr(sys, "frozen", False)


def _browser_installed() -> bool:
    """Check if patchright chromium browser is installed."""
    pw_dir = os.path.join(os.environ.get("LOCALAPPDATA", ""), "ms-playwright")
    if not os.path.isdir(pw_dir):
        return False
    # Look for any chromium-* directory
    for name in os.listdir(pw_dir):
        if name.startswith("chromium-") and os.path.isdir(os.path.join(pw_dir, name)):
            return True
    return False


def _install_browser_cli() -> bool:
    """Install patchright chromium browser via CLI."""
    try:
        from patchright._impl._driver import compute_driver_executable, get_driver_env
        node_exe, cli_js = compute_driver_executable()
        env = get_driver_env()
        result = subprocess.run(
            [str(node_exe), str(cli_js), "install", "chromium"],
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        return result.returncode == 0
    except Exception as e:
        print(f"Browser install failed: {e}")
        return False


def _show_browser_installer() -> bool:
    """Show tkinter UI for browser installation (first-run setup)."""
    try:
        import tkinter as tk
        from tkinter import ttk
    except ImportError:
        return _install_browser_cli()

    root = tk.Tk()
    root.title("ArabLocal Scraper — First Run Setup")
    root.geometry("480x280")
    root.resizable(False, False)
    root.configure(bg="#0c0c0e")

    tk.Label(
        root,
        text="ArabLocal Scraper",
        font=("Segoe UI", 18, "bold"),
        fg="#e4e4e7",
        bg="#0c0c0e",
    ).pack(pady=(30, 5))

    tk.Label(
        root,
        text="Installing browser engine (one-time setup)...",
        font=("Segoe UI", 11),
        fg="#88888f",
        bg="#0c0c0e",
    ).pack(pady=(0, 20))

    progress = ttk.Progressbar(root, length=320, mode="indeterminate")
    progress.pack(pady=10)

    status_var = tk.StringVar(value="Downloading Chromium (~150 MB)...")
    status_label = tk.Label(
        root,
        textvariable=status_var,
        font=("Segoe UI", 10),
        fg="#34d399",
        bg="#0c0c0e",
    )
    status_label.pack(pady=10)

    restart_btn = tk.Button(
        root,
        text="Launch App",
        font=("Segoe UI", 12, "bold"),
        fg="#0c0c0e",
        bg="#34d399",
        activebackground="#5eead4",
        command=lambda: _restart(root),
        state="disabled",
    )
    restart_btn.pack(pady=10)

    install_ok = [False]

    def _restart(r):
        r.destroy()
        if _is_frozen():
            subprocess.Popen([sys.executable])
        else:
            subprocess.Popen([sys.executable] + sys.argv)
        sys.exit(0)

    def _do_install():
        import threading

        def _run():
            progress.start(15)
            ok = _install_browser_cli()
            progress.stop()
            install_ok[0] = ok

            if ok:
                progress.config(mode="determinate")
                progress["value"] = 100
                status_var.set("Browser installed! Click to launch.")
                status_label.config(fg="#34d399")
                restart_btn.config(state="normal")
            else:
                status_var.set("Installation failed. Check internet connection.")
                status_label.config(fg="#f87171")

        threading.Thread(target=_run, daemon=True).start()

    root.after(500, _do_install)
    root.mainloop()
    return install_ok[0]


def check_missing() -> list[tuple[str, str]]:
    """Return list of (import_name, pip_name) for missing packages."""
    missing = []
    for import_name, pip_name in REQUIRED_PACKAGES.items():
        try:
            importlib.import_module(import_name)
        except ImportError:
            missing.append((import_name, pip_name))
    return missing


def install_packages_cli(missing: list[tuple[str, str]]) -> bool:
    """Install missing packages via pip (no UI, for headless mode)."""
    for import_name, pip_name in missing:
        print(f"Installing {pip_name}...")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", pip_name],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"Failed to install {pip_name}: {result.stderr}")
            return False
        print(f"  Installed {pip_name}")
    return True


def show_installer_gui(missing: list[tuple[str, str]]) -> bool:
    """Show tkinter-based installer for missing packages.

    Returns True if all packages installed successfully and restart is needed.
    """
    try:
        import tkinter as tk
        from tkinter import ttk
    except ImportError:
        # No tkinter available -- fall back to CLI install
        return install_packages_cli(missing)

    root = tk.Tk()
    root.title("ArabLocal Scraper -- Installing Dependencies")
    root.geometry("500x400")
    root.resizable(False, False)
    root.configure(bg="#1e1e2e")

    # Title
    tk.Label(
        root,
        text="ArabLocal Scraper",
        font=("Segoe UI", 18, "bold"),
        fg="#cdd6f4",
        bg="#1e1e2e",
    ).pack(pady=(20, 5))

    tk.Label(
        root,
        text="Installing required packages...",
        font=("Segoe UI", 11),
        fg="#a6adc8",
        bg="#1e1e2e",
    ).pack(pady=(0, 15))

    # Package list frame
    frame = tk.Frame(root, bg="#1e1e2e")
    frame.pack(padx=30, fill="x")

    labels = {}
    progress_bars = {}
    status_labels = {}

    for i, (import_name, pip_name) in enumerate(missing):
        tk.Label(
            frame,
            text=pip_name,
            font=("Consolas", 10),
            fg="#cdd6f4",
            bg="#1e1e2e",
            anchor="w",
        ).grid(row=i, column=0, sticky="w", pady=4)

        pb = ttk.Progressbar(frame, length=200, mode="indeterminate")
        pb.grid(row=i, column=1, padx=10, pady=4)
        progress_bars[import_name] = pb

        sl = tk.Label(
            frame,
            text="Waiting",
            font=("Segoe UI", 9),
            fg="#a6adc8",
            bg="#1e1e2e",
            width=12,
        )
        sl.grid(row=i, column=2, pady=4)
        status_labels[import_name] = sl

    # Status message
    status_msg = tk.StringVar(value="Starting installation...")
    tk.Label(
        root,
        textvariable=status_msg,
        font=("Segoe UI", 10),
        fg="#a6e3a1",
        bg="#1e1e2e",
    ).pack(pady=15)

    # Restart button (hidden initially)
    restart_btn = tk.Button(
        root,
        text="Restart Application",
        font=("Segoe UI", 12, "bold"),
        fg="#1e1e2e",
        bg="#a6e3a1",
        activebackground="#94e2d5",
        command=lambda: _restart(root),
        state="disabled",
    )
    restart_btn.pack(pady=10)

    install_success = [True]

    def _restart(r):
        r.destroy()
        # Relaunch the application
        subprocess.Popen([sys.executable] + sys.argv)
        sys.exit(0)

    def _install_thread():
        import threading

        def _run():
            for import_name, pip_name in missing:
                progress_bars[import_name].start(15)
                status_labels[import_name].config(text="Installing...", fg="#f9e2af")
                status_msg.set(f"Installing {pip_name}...")

                result = subprocess.run(
                    [sys.executable, "-m", "pip", "install", pip_name],
                    capture_output=True,
                    text=True,
                )

                progress_bars[import_name].stop()

                if result.returncode == 0:
                    progress_bars[import_name].config(mode="determinate")
                    progress_bars[import_name]["value"] = 100
                    status_labels[import_name].config(text="Done", fg="#a6e3a1")
                else:
                    progress_bars[import_name].config(mode="determinate")
                    progress_bars[import_name]["value"] = 0
                    status_labels[import_name].config(text="Failed", fg="#f38ba8")
                    install_success[0] = False

            if install_success[0]:
                status_msg.set("All packages installed! Please restart.")
                restart_btn.config(state="normal")
            else:
                status_msg.set("Some packages failed to install. Check logs.")

        threading.Thread(target=_run, daemon=True).start()

    # Start installation after a short delay
    root.after(500, _install_thread)
    root.mainloop()

    return install_success[0]


def ensure_dependencies() -> bool:
    """Check and install dependencies. Returns True if app can proceed.

    In EXE mode: only checks for browser binaries.
    In dev mode: checks Python packages first, then browser binaries.
    """
    if not _is_frozen():
        # Dev mode -- check Python packages
        missing = check_missing()
        if missing:
            print(f"Missing packages: {[p for _, p in missing]}")
            ok = show_installer_gui(missing)
            if not ok:
                return False

    # Check browser binaries (both EXE and dev mode)
    if not _browser_installed():
        print("Browser not installed -- running first-time setup...")
        return _show_browser_installer()

    return True
