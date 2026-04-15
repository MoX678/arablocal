# ArabLocal Scraper

Multi-country business listing scraper for [ArabLocal.com](https://arablocal.com) with a PyQt6 desktop GUI and CLI interface. Extracts business names, phones, emails, websites, social links, addresses, and more from local business directories across the Middle East.

## Supported Countries

| Key | Country      | Source                          |
|-----|--------------|---------------------------------|
| UAE | United Arab Emirates | uae.arablocal.com        |
| KW  | Kuwait       | kuwaitlocal.com                 |
| SA  | Saudi Arabia | arablocal.com                   |
| QA  | Qatar        | qatar.arablocal.com             |
| OM  | Oman         | oman.arablocal.com              |

## Features

- **GUI mode** — PyQt6 desktop app with dashboard, country selector, proxy manager, and live results viewer
- **CLI mode** — headless scraper for automation and scripting
- **Multi-country parallel scraping** — run multiple countries simultaneously
- **Category auto-discovery** — detects available business categories per country
- **Checkpoint resume** — SQLite staging DB allows resuming interrupted scrapes
- **Proxy support** — load and test proxy lists, rotate per request
- **Adaptive delays** — rate-limiting avoidance with jittered backoff
- **Per-category CSV export** — organized output by country and category
- **Browser-based extraction** — uses Chromium via patchright/scrapling for JS-rendered pages

## Installation

### Requirements

- Python 3.10+
- Chromium browser (auto-installed on first run)

### From Source

```bash
git clone <repo-url>
cd arablocal
pip install -r requirements.txt
```

On first run, the app will prompt to install the Chromium browser if not already present.

### Proxy Setup

Copy the example file and add your proxies (one per line, `IP:PORT`):

```bash
cp proxies.txt.example proxies.txt
```

## Usage

### GUI Mode

```bash
python -m gui.app
```

### CLI Mode

```bash
python scrapper.py --country uae --fresh
python scrapper.py --country kw,sa --concurrency 5
```

## Building Windows EXE

```bash
# Install PyInstaller if needed
pip install pyinstaller

# Build using the spec file
pyinstaller arablocal.spec --noconfirm

# Or use the build script
build.bat
```

The EXE is output to `dist/ArabLocal/ArabLocal.exe`.

> **Note:** The built EXE requires Chromium browser binaries. If running on a fresh machine, run `python -m patchright install chromium` first, or copy the `ms-playwright` folder from `%LOCALAPPDATA%\ms-playwright`.

## Project Structure

```
arablocal/
├── core/               # Scraping engine
│   ├── config.py       # URLs, country info, field definitions
│   ├── delay.py        # Adaptive rate limiting
│   ├── discovery.py    # Category auto-discovery
│   ├── engine.py       # Main scraping pipeline
│   ├── extraction.py   # Business data extraction
│   └── storage.py      # SQLite + CSV persistence
├── gui/                # PyQt6 desktop application
│   ├── app.py          # Entry point
│   ├── bootstrap.py    # Dependency checker / first-run setup
│   ├── main_window.py  # Sidebar navigation window
│   ├── theme.py        # Dark theme + stylesheet
│   ├── workers.py      # QThread workers for scraping
│   └── pages/          # Dashboard, Scrape, Proxy, Results pages
├── output/             # Scraped data (gitignored)
├── scrapper.py         # CLI entry point
├── proxy_tester.py     # Standalone proxy testing utility
├── arablocal.spec      # PyInstaller build spec
├── build.bat           # Windows build script
├── requirements.txt    # Python dependencies
└── pyproject.toml      # Package metadata
```

## Output Format

Scraped data is saved to `output/{country}/`:
- `{country}_raw.csv` — incremental raw append
- `{country}_all.csv` — deduplicated combined export
- `categories/{category}.csv` — per-category sorted exports
- `logs/` — scrape session logs
