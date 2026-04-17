"""Auto-updater — check GitHub releases and apply updates."""

from __future__ import annotations

import json
import logging
import os
import re as _re
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

# Read-only fine-grained PAT — contents:read on MoX678/arablocal only.
# This token can ONLY read releases. It cannot push, delete, or modify anything.
_GITHUB_TOKEN: Optional[str] = None

# ─── DPAPI encryption helpers (Windows only) ─────────────────────────────

_DPAPI_TOKEN_DIR = os.path.join(os.environ.get("APPDATA", ""), "ArabLocal")
_DPAPI_TOKEN_FILE = os.path.join(_DPAPI_TOKEN_DIR, "github_token.dat")


def _dpapi_encrypt(plaintext: str) -> Optional[bytes]:
    """Encrypt a string with Windows DPAPI. Returns ciphertext bytes or None."""
    try:
        import win32crypt  # type: ignore
        return win32crypt.CryptProtectData(
            plaintext.encode("utf-8"), "ArabLocal-Token",
            None, None, None, 0
        )
    except Exception:
        return None


def _dpapi_decrypt(ciphertext: bytes) -> Optional[str]:
    """Decrypt DPAPI ciphertext. Returns plaintext or None."""
    try:
        import win32crypt  # type: ignore
        _, data = win32crypt.CryptUnprotectData(ciphertext, None, None, None, 0)
        return data.decode("utf-8")
    except Exception:
        return None


def _load_dpapi_token() -> Optional[str]:
    """Try loading the DPAPI-encrypted token from %APPDATA%."""
    try:
        if os.path.isfile(_DPAPI_TOKEN_FILE):
            with open(_DPAPI_TOKEN_FILE, "rb") as f:
                ciphertext = f.read()
            if ciphertext:
                return _dpapi_decrypt(ciphertext)
    except Exception:
        pass
    return None


def _save_dpapi_token(token: str) -> bool:
    """Encrypt and save token via DPAPI. Returns True on success."""
    try:
        ciphertext = _dpapi_encrypt(token)
        if ciphertext is None:
            return False
        os.makedirs(_DPAPI_TOKEN_DIR, exist_ok=True)
        with open(_DPAPI_TOKEN_FILE, "wb") as f:
            f.write(ciphertext)
        return True
    except Exception:
        return False


def _get_github_token() -> Optional[str]:
    """Load the GitHub token (read-only PAT for release checks).

    Checks in order:
    1. Module-level _GITHUB_TOKEN (cached)
    2. ARABLOCAL_GITHUB_TOKEN env var
    3. DPAPI-encrypted token in %APPDATA%/ArabLocal/github_token.dat
    4. .github_token plaintext file next to the executable

    If a plaintext token is found and DPAPI is available, it is automatically
    migrated to encrypted storage.
    """
    global _GITHUB_TOKEN
    if _GITHUB_TOKEN:
        return _GITHUB_TOKEN

    # Env var
    token = os.environ.get("ARABLOCAL_GITHUB_TOKEN", "").strip()
    if token:
        _GITHUB_TOKEN = token
        return token

    # DPAPI encrypted store
    token = _load_dpapi_token()
    if token:
        _GITHUB_TOKEN = token
        return token

    # Plaintext file next to exe
    if getattr(sys, "frozen", False):
        token_path = os.path.join(os.path.dirname(sys.executable), ".github_token")
    else:
        token_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            ".github_token",
        )
    try:
        if os.path.isfile(token_path):
            with open(token_path, "r") as f:
                token = f.read().strip()
            if token:
                _GITHUB_TOKEN = token
                # Auto-migrate to DPAPI if available
                if _save_dpapi_token(token):
                    log.info("GitHub token migrated to encrypted storage (DPAPI)")
                return token
    except Exception:
        pass

    return None


def _make_request(url: str, timeout: int = 10) -> urllib.request.Request:
    """Build a request with auth headers if a token is available."""
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "ArabLocal-Scraper-Updater",
    }
    token = _get_github_token()
    if token:
        headers["Authorization"] = f"token {token}"
    return urllib.request.Request(url, headers=headers)


def _parse_version(tag: str) -> tuple[int, ...]:
    """Parse 'v3.0.1' or '3.0.1-beta' into (3, 0, 1).

    Strips any pre-release suffix (e.g. '-beta', '-rc1') before parsing.
    """
    clean = _re.split(r"[^0-9.]", tag.lstrip("v"), maxsplit=1)[0].rstrip(".")
    if not clean:
        return (0,)
    return tuple(int(x) for x in clean.split(".") if x.isdigit())


def _is_prerelease(data: dict) -> bool:
    """Check if a release is a pre-release (alpha, beta, rc, draft)."""
    if data.get("prerelease", False) or data.get("draft", False):
        return True
    tag = data.get("tag_name", "")
    return bool(_re.search(r"(alpha|beta|rc|dev|pre)", tag, _re.IGNORECASE))


def check_for_update(current_version: str) -> Optional[dict]:
    """Check GitHub for a newer stable release.

    Returns dict with update info or None if up-to-date.
    Skips pre-release/draft versions.
    """
    try:
        req = _make_request(GITHUB_API)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())

        tag = data.get("tag_name", "")
        if not tag:
            return None

        # Skip pre-releases
        if _is_prerelease(data):
            log.debug(f"Skipping pre-release: {tag}")
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
                # Use API URL for private repos (browser_download_url fails on redirects)
                download_url = asset.get("url") or asset["browser_download_url"]
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
    except urllib.error.HTTPError as e:
        if e.code == 401:
            log.warning("Update check failed: invalid GitHub token (401 Unauthorized)")
        elif e.code == 403:
            log.warning("Update check failed: API rate limit or forbidden (403)")
        elif e.code == 404:
            log.warning("Update check failed: repository not found (404)")
        else:
            log.warning(f"Update check failed: HTTP {e.code}")
        return None
    except urllib.error.URLError as e:
        log.warning(f"Update check failed: network error — {e.reason}")
        return None
    except Exception as e:
        log.warning(f"Update check failed: {e}")
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
        req = _make_request(url, timeout=300)
        # Override Accept for binary download from GitHub API asset URL
        req.remove_header("Accept")
        req.add_header("Accept", "application/octet-stream")
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

        # Validate the zip file
        if not zipfile.is_zipfile(zip_path):
            log.error("Downloaded file is not a valid zip archive")
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return None

        with zipfile.ZipFile(zip_path, "r") as zf:
            bad = zf.testzip()
            if bad:
                log.error(f"Corrupt file in zip: {bad}")
                shutil.rmtree(tmp_dir, ignore_errors=True)
                return None

        return zip_path
    except urllib.error.HTTPError as e:
        log.error(f"Update download failed: HTTP {e.code}")
        return None
    except urllib.error.URLError as e:
        log.error(f"Update download failed: network error — {e.reason}")
        return None
    except Exception as e:
        log.error(f"Update download failed: {e}")
        return None


def apply_update(zip_path: str, flush_callback=None) -> bool:
    """Extract update zip and launch updater batch script.

    The batch script:
    1. Waits for the current process to exit
    2. Copies new files over the old installation
    3. Cleans up temp files
    4. Relaunches the app

    Args:
        zip_path: Path to the downloaded update zip.
        flush_callback: Optional callable to flush any pending data (e.g., CSV buffers).

    Returns True if the updater was launched (caller should exit).
    """
    try:
        # Flush pending data before applying update
        if flush_callback:
            try:
                flush_callback()
            except Exception as e:
                log.warning(f"Pre-update flush failed: {e}")
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

        update_log = os.path.join(app_dir, "errors", "_update.log")
        bat_content = f"""@echo off
title ArabLocal Updater
set LOGFILE="{update_log}"
echo [%date% %time%] Update started >> %LOGFILE%
echo Waiting for application to close...
:waitloop
tasklist /FI "PID eq {pid}" 2>NUL | find /I "{pid}" >NUL
if %ERRORLEVEL%==0 (
    timeout /t 1 /nobreak >NUL
    goto waitloop
)
echo [%date% %time%] Application closed >> %LOGFILE%
echo Application closed. Applying update...
timeout /t 2 /nobreak >NUL
xcopy /s /y /q "{source_dir}\\*" "{app_dir}\\"
if errorlevel 1 (
    echo [%date% %time%] xcopy FAILED >> %LOGFILE%
    echo Update failed! Press any key to exit.
    pause
    exit /b 1
)
echo [%date% %time%] Files copied OK >> %LOGFILE%
echo Update complete! Launching app...
echo [%date% %time%] Launching: {os.path.join(app_dir, exe_name)} >> %LOGFILE%
start "" /D "{app_dir}" "{os.path.join(app_dir, exe_name)}"
echo [%date% %time%] Start command returned %ERRORLEVEL% >> %LOGFILE%
echo Cleaning up...
timeout /t 2 /nobreak >NUL
rmdir /s /q "{extract_dir}" 2>NUL
del /q "{zip_path}" 2>NUL
echo [%date% %time%] Cleanup done >> %LOGFILE%
(goto) 2>nul & del "%~f0"
"""
        with open(bat_path, "w", encoding="mbcs", errors="replace") as f:
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
