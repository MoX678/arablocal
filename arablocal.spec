# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for ArabLocal Scraper GUI.

Build:
    pyinstaller arablocal.spec

The output goes to dist/ArabLocal/ (onedir mode).
After building, run:  dist\\ArabLocal\\ArabLocal.exe
Browser binaries NOT bundled — run first-launch install or copy ms-playwright cache.
"""

import os
import sys
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

# Collect patchright driver (node.exe + package files ~86 MB)
patchright_datas = collect_data_files("patchright", subdir="driver")

# Collect browserforge data (fingerprint profiles)
browserforge_datas = collect_data_files("browserforge")

# Collect apify_fingerprint_datapoints (zip data for browserforge bayesian network)
apify_datas = collect_data_files("apify_fingerprint_datapoints")

# Collect scrapling internal data
scrapling_datas = collect_data_files("scrapling")

# Hidden imports for async + browser engine
hidden_imports = [
    *collect_submodules("scrapling"),
    *collect_submodules("patchright"),
    *collect_submodules("browserforge"),
    "playwright",
    "playwright.sync_api",
    "playwright.async_api",
    "httpx",
    "httpx._transports",
    "anyio",
    "anyio._backends",
    "anyio._backends._asyncio",
    "lxml",
    "lxml.html",
    "lxml.etree",
    "cssselect",
    "orjson",
    "tld",
    "rich",
    "rich.console",
    "rich.progress",
    "apify_fingerprint_datapoints",
    "sqlite3",
    "asyncio",
    "logging",
]

a = Analysis(
    ["gui/app.py"],
    pathex=[os.path.abspath(".")],
    binaries=[],
    datas=[
        *patchright_datas,
        *browserforge_datas,
        *apify_datas,
        *scrapling_datas,
        # Bundle proxies template if exists
        ("proxies.txt", "."),
    ],
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "matplotlib",
        "scipy",
        "numpy",
        "pandas",
        "IPython",
        "jupyter",
        "notebook",
        "pytest",
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="ArabLocal",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,  # Keep console for debugging; set to False for release
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # icon="icon.ico",  # Uncomment when you have an icon
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="ArabLocal",
)
