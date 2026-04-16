@echo off
REM ──────────────────────────────────────────
REM  ArabLocal Scraper — Build Standalone EXE
REM ──────────────────────────────────────────

echo [1/3] Checking PyInstaller...
pip show pyinstaller >nul 2>&1
if errorlevel 1 (
    echo Installing PyInstaller...
    pip install pyinstaller
)

echo [2/3] Building ArabLocal.exe...
pyinstaller arablocal.spec --noconfirm

if errorlevel 1 (
    echo.
    echo BUILD FAILED. Check errors above.
    pause
    exit /b 1
)

echo [3/3] Bundling config files...
if exist ".github_token" (
    copy /y ".github_token" "dist\ArabLocal\.github_token" >nul
    echo      .github_token copied to dist.
)

echo [4/4] Checking browser installation...
echo.
echo ══════════════════════════════════════════════
echo  BUILD COMPLETE — dist\ArabLocal\ArabLocal.exe
echo ══════════════════════════════════════════════
echo.
echo  IMPORTANT: The EXE needs Chromium browser installed.
echo  If this is a fresh machine, run:
echo.
echo    python -m patchright install chromium
echo.
echo  Or copy the ms-playwright folder from:
echo    %%LOCALAPPDATA%%\ms-playwright
echo  to the target machine's same location.
echo.
pause
