"""ArabLocal Scraper Core Package — modular scraping engine."""

__version__ = "3.2.5"

from core.config import JobConfig, BASE_URLS, COUNTRY_INFO, resolve_concurrency
from core.delay import AdaptiveDelay
from core.engine import ArabLocalEngine

__all__ = [
    "JobConfig",
    "BASE_URLS",
    "COUNTRY_INFO",
    "resolve_concurrency",
    "AdaptiveDelay",
    "ArabLocalEngine",
]
