"""Constants, configuration, and job specification for the ArabLocal scraper."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


# ─── Target URLs ─────────────────────────────────────────────────────────────
BASE_URLS = {
    "uae": "https://uae.arablocal.com",
    "kw":  "https://kuwaitlocal.com",
    "sa":  "https://arablocal.com",
    "qa":  "https://qatar.arablocal.com",
    "om":  "https://oman.arablocal.com",
}

COUNTRY_INFO = {
    "uae": {"code": "+971", "name": "UAE"},
    "kw":  {"code": "+965", "name": "Kuwait"},
    "sa":  {"code": "+966", "name": "Saudi Arabia"},
    "qa":  {"code": "+974", "name": "Qatar"},
    "om":  {"code": "+968", "name": "Oman"},
}

# ─── Social & Link Classification ────────────────────────────────────────────
SOCIAL_DOMAINS = {
    "facebook.com": "Facebook",
    "fb.com":       "Facebook",
    "twitter.com":  "Twitter",
    "x.com":        "Twitter",
    "instagram.com":"Instagram",
    "linkedin.com": "LinkedIn",
    "snapchat.com": "Snapchat",
    "tiktok.com":   "TikTok",
    "youtube.com":  "YouTube",
    "pinterest.com":"Pinterest",
    "t.me":         "Telegram",
}

# ArabLocal's own social accounts — must be excluded from business profiles
SITE_SOCIAL_BLACKLIST = {
    # UAE
    "arablocaluae971", "arablocal2020", "uaelocalsearch",
    "UC8BTQhTUZMoR0IgxZ6zsmNA",
    # Kuwait
    "kuwaitlocal965", "kuwaitlocal", "KuwaitLocal",
    # Oman
    "arablocaloman", "UCg5UWsTvOlwEvEdbEqZhdmQ",
    # Generic / cross-site
    "arablocal", "company/9552438",
    # Site-owned Telegram
    "+eabbIq4Qtq5iOTU0",
}

IGNORE_DOMAINS = {
    "arablocal.com", "kuwaitlocal.com", "kuwaitadvert.com",
    "google.com", "google.ae",
    "gstatic.com", "cloudflare.com", "googleapis.com", "goo.gl",
    "bit.ly", "galli2delhi.com",
}

# Merge social domains into ignore set for website extraction
IGNORE_FOR_WEBSITE = IGNORE_DOMAINS | set(SOCIAL_DOMAINS.keys())

# ─── Raw CSV column ordering ─────────────────────────────────────────────────
BASE_FIELDS = [
    "Name", "Phone_1", "Phone_2", "Phone_3", "WhatsApp", "Email",
    "Location", "Area", "Governorate",
    "Country", "About", "Website", "Rating", "Views", "Fax",
]

# Sentinel value to signal consumer tasks to shut down
_SENTINEL = object()


# ─── Job Configuration ───────────────────────────────────────────────────────

@dataclass
class JobConfig:
    """Configuration for a single country scraping job.

    This is the API contract between the CLI (or future UI) and the engine.
    Each JobConfig results in one ArabLocalEngine instance with its own
    browser session, database, and output directory.
    """
    country: str
    categories: list[str] = field(default_factory=list)  # empty = all
    limit: int = 0                   # 0 = unlimited
    output_dir: str = "output"       # per-country output root
    concurrency: int = 4             # concurrent browser tabs
    proxy_file: Optional[str] = None
    fresh: bool = False              # ignore checkpoints
    dry_run: bool = False            # discover categories only
    verbose: bool = False
    quiet: bool = False

    @property
    def country_key(self) -> str:
        return self.country

    @property
    def base_url(self) -> str:
        return BASE_URLS.get(self.country, BASE_URLS["uae"])

    @property
    def phone_prefix(self) -> str:
        return COUNTRY_INFO.get(self.country, COUNTRY_INFO["uae"])["code"]

    @property
    def country_name(self) -> str:
        return COUNTRY_INFO.get(self.country, COUNTRY_INFO["uae"])["name"]

    @property
    def is_kuwait(self) -> bool:
        return self.country == "kw"

    @property
    def needs_cloudflare(self) -> bool:
        """Disabled — no countries currently need CF solving."""
        return False

    @property
    def db_path(self) -> str:
        return os.path.join(self.output_dir, f"{self.country}_staging.db")

    @property
    def raw_csv_path(self) -> str:
        return os.path.join(self.output_dir, f"{self.country}_raw.csv")

    @property
    def categories_dir(self) -> str:
        return os.path.join(self.output_dir, "categories")

    @property
    def log_dir(self) -> str:
        return os.path.join(self.output_dir, "logs")

    @property
    def all_csv_path(self) -> str:
        return os.path.join(self.output_dir, f"{self.country}_all.csv")

    def ensure_dirs(self):
        """Create all output directories if they don't exist."""
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.categories_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)

    def validate(self):
        """Validate the job config. Raises ValueError on invalid config."""
        if self.country not in BASE_URLS:
            valid = ", ".join(sorted(BASE_URLS.keys()))
            raise ValueError(f"Unknown country '{self.country}'. Valid: {valid}")
        if self.limit < 0:
            raise ValueError(f"Limit must be >= 0, got {self.limit}")
        if self.concurrency < 1:
            raise ValueError(f"Concurrency must be >= 1, got {self.concurrency}")


# ─── Concurrency Resolution ──────────────────────────────────────────────────

def resolve_concurrency(threads_arg: str, num_jobs: int = 1) -> int:
    """Resolve the --threads argument into per-job concurrency.

    Args:
        threads_arg: "auto" (75% CPU), "max" (all cores), or an integer string.
        num_jobs: Number of parallel country jobs to split concurrency across.

    Returns:
        Per-job concurrency (minimum 1).
    """
    cpu = os.cpu_count() or 4

    if threads_arg == "auto":
        total = max(1, int(cpu * 0.75))
    elif threads_arg == "max":
        total = cpu
    else:
        try:
            total = int(threads_arg)
        except (ValueError, TypeError):
            total = max(1, int(cpu * 0.75))
        total = max(1, min(total, cpu * 2))  # Cap at 2x CPU

    per_job = max(1, total // max(1, num_jobs))
    return per_job


def parse_job_spec(spec: str) -> dict:
    """Parse a --job spec string into components.

    Format: country[:limit[:cat1,cat2,...]]

    Examples:
        "sa"                    → {"country": "sa", "limit": 0, "categories": []}
        "sa:5"                  → {"country": "sa", "limit": 5, "categories": []}
        "sa:5:health-care"      → {"country": "sa", "limit": 5, "categories": ["health-care"]}
        "sa:0:health-care,food" → {"country": "sa", "limit": 0, "categories": ["health-care", "food"]}

    Raises:
        ValueError: If the spec is malformed.
    """
    parts = spec.split(":", 2)
    country = parts[0].strip().lower()
    if not country:
        raise ValueError(f"Empty country in job spec: '{spec}'")
    if country not in BASE_URLS:
        valid = ", ".join(sorted(BASE_URLS.keys()))
        raise ValueError(f"Unknown country '{country}' in job spec '{spec}'. Valid: {valid}")

    limit = 0
    if len(parts) > 1 and parts[1].strip():
        try:
            limit = int(parts[1].strip())
        except ValueError:
            raise ValueError(f"Invalid limit '{parts[1]}' in job spec '{spec}'. Must be integer.")
        if limit < 0:
            raise ValueError(f"Limit must be >= 0, got {limit} in job spec '{spec}'")

    categories = []
    if len(parts) > 2 and parts[2].strip():
        categories = [c.strip() for c in parts[2].split(",") if c.strip()]

    return {"country": country, "limit": limit, "categories": categories}
