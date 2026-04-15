"""Auto-updater — check GitHub releases and apply updates."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from typing import Optional

log = logging.getLogger("arablocal")

GITHUB_REPO = "MoX678/arablocal"
GITHUB_API = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
ASSET_PATTERN = "ArabLocal-v{version}-win64.zip"


def _parse_version(tag: str) -> tuple[int, ...]:
    """Parse 'v3.0.1' or '3.0.1' into (3, 0, 1)."""
    return tuple(int(x) for x in tag.lstrip("v").split("."))


def check_for_update(current_version: str) -> Optional[dict]:
    """Check GitHub for a newer release.

    Returns dict with update info or None if up-to-date.
    """
    try:
        req = urllib.request.Request(
            GITHUB_API,
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": "ArabLocal-Scraper-Updater",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())

        tag = data.get("tag_name", "")
        if not tag:
            return None

        remote_ver = _parse_version(tag)
        local_ver = _parse_version(current_version)

        if remote_ver <= local_ver:
            return None

        # Find the win64 zip asset
        download_url = None
        asset_size = 0
        for asset in data.get("assets", []):
            name = asset.get("name", "")
            if name.endswith("-win64.zip") and name.startswith("ArabLocal-"):
                download_url = asset["browser_download_url"]
                asset_size = asset.get("size", 0)
                break

        if not download_url:
            return None

        return {
            "version": tag.lstrip("v"),
            "tag": tag,
            "changelog": data.get("body", ""),
            "download_url": download_url,
            "asset_size": asset_size,
            "html_url": data.get("html_url", ""),
        }
    except Exception as e:
        log.debug(f"Update check failed: {e}")
        return None


def download_update(url: str, progress_callback=None) -> Optional[str]:
    """Download the update zip to a temp directory.

    Args:
        url: Download URL for the zip asset.
        progress_callback: Optional callable(bytes_downloaded, total_bytes).

    Returns:
        Path to the downloaded zip file, or None on failure.
    """
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "ArabLocal-Scraper-Updater"},
        )
        with urllib.request.urlopen(req, timeout=300) as resp:
            total = int(resp.headers.get("Content-Length", 0))
            tmp_dir = tempfile.mkdtemp(prefix="arablocal_update_")
            zip_path = os.path.join(tmp_dir, "update.zip")

            downloaded = 0
            with open(zip_path, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback:
                        progress_callback(downloaded, total)

        return zip_path
    except Exception as e:
        log.error(f"Update download failed: {e}")
        return None


def apply_update(zip_path: str) -> bool:
    """Extract update zip and launch updater batch script.

    The batch script:
    1. Waits for the current process to exit
    2. Copies new files over the old installation
    3. Cleans up temp files
    4. Relaunches the app

    Returns True if the updater was launched (caller should exit).
    """
    try:
        # Determine app directory (where the exe lives)
        if getattr(sys, "frozen", False):
            app_dir = os.path.dirname(sys.executable)
            exe_name = os.path.basename(sys.executable)
        else:
            app_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            exe_name = "ArabLocal.exe"

        # Extract zip to temp
        extract_dir = os.path.join(os.path.dirname(zip_path), "extracted")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

        # Find the inner folder (e.g., ArabLocal/)
        contents = os.listdir(extract_dir)
        if len(contents) == 1 and os.path.isdir(os.path.join(extract_dir, contents[0])):
            source_dir = os.path.join(extract_dir, contents[0])
        else:
            source_dir = extract_dir

        # Write updater batch script
        bat_path = os.path.join(os.path.dirname(zip_path), "_update.bat")
        pid = os.getpid()

        bat_content = f"""@echo off
title ArabLocal Updater
echo Waiting for application to close...
:waitloop
tasklist /FI "PID eq {pid}" 2>NUL | find /I "{pid}" >NUL
if not errorlevel 1 (
    timeout /t 1 /nobreak >NUL
    goto waitloop
)
echo Applying update...
xcopy /s /y /q "{source_dir}\\*" "{app_dir}\\"
if errorlevel 1 (
    echo Update failed! Press any key to exit.
    pause
    exit /b 1
)
echo Cleaning up...
rmdir /s /q "{os.path.dirname(zip_path)}"
echo Update complete! Launching app...
start "" "{os.path.join(app_dir, exe_name)}"
del "%~f0"
"""
        with open(bat_path, "w", encoding="utf-8") as f:
            f.write(bat_content)

        # Launch updater (hidden window)
        subprocess.Popen(
            ["cmd", "/c", bat_path],
            creationflags=subprocess.CREATE_NEW_CONSOLE,
            close_fds=True,
        )

        log.info("Updater launched — app will restart after update.")
        return True

    except Exception as e:
        log.error(f"Failed to apply update: {e}")
        # Clean up
        try:
            shutil.rmtree(os.path.dirname(zip_path), ignore_errors=True)
        except Exception:
            pass
        return False
