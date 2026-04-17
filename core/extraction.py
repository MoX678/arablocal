"""Business data extraction from scraped pages."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Dict
from urllib.parse import urlparse

from core.config import (
    SOCIAL_DOMAINS, SITE_SOCIAL_BLACKLIST,
    IGNORE_FOR_WEBSITE,
)


# ─── Arabic normalisation helpers for fingerprinting ──────────────────────

_TASHKEEL = re.compile(r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC"
                        r"\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]")
_ALEF_VARIANTS = re.compile(r"[\u0622\u0623\u0625\u0671]")  # آ أ إ ٱ → ا


def _normalize_arabic(text: str) -> str:
    """Strip tashkeel, normalise alef forms, collapse whitespace."""
    text = _TASHKEEL.sub("", text)
    text = _ALEF_VARIANTS.sub("\u0627", text)  # → ا
    text = unicodedata.normalize("NFKC", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def compute_fingerprint(data: Dict[str, str], country: str) -> str:
    """Compute a 16-char hex fingerprint for deduplication.

    Uses SHA-256 of normalised (name | phone | country). Returns "" if
    both name and phone are empty (no meaningful identity).
    """
    name = _normalize_arabic(data.get("Name", ""))
    phone = re.sub(r"[^\d+]", "", data.get("Phone_1", ""))
    if not name and not phone:
        return ""
    raw = f"{name}|{phone}|{country.lower()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def extract_data(page, url: str, phone_prefix: str, country_name: str) -> Dict[str, str]:
    """Extract all business fields from an ArabLocal detail page.

    Works for UAE, SA, BH, QA, OM. For Kuwait, use extract_data_kuwait().

    Args:
        page: Scrapling response object with CSS selector support.
        url: The business page URL.
        phone_prefix: Country phone prefix (e.g. "+971").
        country_name: Human-readable country name (e.g. "Saudi Arabia").

    Returns:
        Dict of extracted fields. Empty values are omitted.
    """
    text_nodes = page.css("body *::text").getall()
    full_text = "\n".join(t.strip() for t in text_nodes if t.strip())

    try:
        body = page.body if hasattr(page, 'body') else b''
        if isinstance(body, str):
            html = body
        elif isinstance(body, bytes):
            html = body.decode(
                getattr(page, "encoding", None) or "utf-8", errors="replace"
            )
        else:
            html = str(body) if body else ""
    except Exception:
        html = ""

    data: Dict[str, str] = {}

    # ── Name ──
    data["Name"] = extract_name(page)

    # ── Phone: collect all numbers, split into Phone_1/2/3 ──
    phones_raw = []
    tel_links = page.css('a[href^="tel:"]::attr(href)').getall()
    for t in tel_links:
        num = t.replace("tel:", "").strip()
        if num:
            phones_raw.append(num)

    if not phones_raw:
        prefix_escaped = re.escape(phone_prefix)
        phones_raw = re.findall(
            rf"{prefix_escaped}[\s-]?\d{{1,3}}[\s-]?\d{{4,9}}(?!\d)", full_text
        )

    seen_p: set = set()
    cleaned_phones = []
    for p in phones_raw:
        c = re.sub(r"\s+", " ", p).strip()
        if c not in seen_p:
            seen_p.add(c)
            cleaned_phones.append(c)
    for i, phone in enumerate(cleaned_phones[:3], 1):
        data[f"Phone_{i}"] = phone

    # ── WhatsApp ──
    wa_links = page.css('a[href*="wa.me"]::attr(href)').getall()
    for wa in wa_links:
        m_wa = re.search(r'wa\.me/(\d+)', wa)
        if m_wa:
            data["WhatsApp"] = f"+{m_wa.group(1)}"
            break

    # ── Email ──
    m = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", full_text)
    if m:
        email = m.group(0)
        if "." in email.split("@")[1]:
            data["Email"] = email

    # ── Location: labelled first (UAE/KW), CSS fallback (SA) ──
    loc = _extract_labelled(full_text, "Address")
    if not loc:
        locs = page.css('.bus_addr span::text, .bus_addr a::text, .location::text').getall()
        seen_loc: list = []
        for l in locs:
            l = l.strip().rstrip(",")
            if l and not l.isspace() and l not in seen_loc:
                seen_loc.append(l)
        loc = ", ".join(seen_loc)
        loc = re.sub(r",\s*,", ",", loc).strip(", ")
    data["Location"] = loc

    area = _extract_labelled(full_text, "Area")
    gov = _extract_labelled(full_text, "Governorate").rstrip(",").strip()

    # For SA pages: parse structured CSS location into Area/Governorate
    if not area and not gov and loc:
        parts = [p.strip() for p in loc.split(",") if p.strip()]
        if len(parts) >= 3:
            area = parts[0]
            gov = parts[-2]
        elif len(parts) == 2:
            area = parts[0]

    data["Area"] = area
    data["Governorate"] = gov

    country = _extract_labelled(full_text, "Country")
    if country:
        country = country.split("\n")[0].strip()
        if len(country) > 50:
            country = country[:50].rsplit(" ", 1)[0]
    data["Country"] = country or country_name

    # ── Fax ──
    for div in page.css("div.bus_addr"):
        if div.css("span.hot_icon"):
            num = div.css("p::text").get("").strip()
            if num and re.match(r"^[\d\s+()-]+$", num):
                data["Fax"] = num
                break
    if "Fax" not in data:
        m = re.search(r"(?:fax|Fax|FAX)\s*[:\s]*([\d\s+()-]+)", full_text)
        if m:
            data["Fax"] = m.group(1).strip()

    # ── Rating ──
    no_rating = page.css("#no_business_comment::text").get("")
    if "No ratings" not in no_rating:
        stars = page.css(".bus_star .star_icon")
        if stars:
            filled = len([s for s in stars if "active" in s.attrib.get("class", "")])
            if filled > 0:
                data["Rating"] = str(filled)
        if "Rating" not in data:
            m = re.search(r"(\d+(?:\.\d+)?)\s*(?:out of\s*\d+|/\s*5)", full_text)
            if m:
                data["Rating"] = m.group(1)

    # ── Views: fa-eye regex on raw HTML, CSS fallback ──
    if html:
        m_views = re.search(r'fa-eye.*?>\s*([\d,]+)\s*<', html)
        if m_views:
            data["Views"] = m_views.group(1).strip().replace(",", "")
    if "Views" not in data:
        view_texts = page.css("span.bus_view *::text").getall()
        for t in view_texts:
            t = t.strip().replace(",", "")
            if t.isdigit():
                data["Views"] = t
                break

    # ── About (truncate to 500 chars) ──
    parts = page.css("div.bus_inner_content_desc *::text").getall()
    about = " ".join(t.strip() for t in parts if t.strip())
    if not about or len(about) <= 10:
        about = page.css("meta[property='og:description']::attr(content)").get("").strip()
    if about and len(about) > 10:
        about = re.sub(r"\s+", " ", about).strip()
        if len(about) > 500:
            about = about[:497] + "..."
        data["About"] = about

    # ── Website: dedicated link first ──
    bus_url = page.css("a.bus_url_a::attr(href)").get("").strip()
    if bus_url:
        data["Website"] = bus_url

    # ── Social links + Website fallback ──
    bus_inner = page.css("div.bus_inner")
    link_scope = bus_inner[0].css("a[href]") if bus_inner else []
    for a in link_scope:
        href = (a.css("::attr(href)").get("") or "").strip()
        if not href or not href.startswith("http"):
            continue
        domain = urlparse(href).netloc.lower()

        if "arablocal" in href.lower():
            continue
        if any(acct in href for acct in SITE_SOCIAL_BLACKLIST):
            continue

        is_social = False
        for soc_domain, platform in SOCIAL_DOMAINS.items():
            if soc_domain in domain:
                if platform not in data:
                    data[platform] = href
                is_social = True
                break

        if not is_social and "Website" not in data:
            if not any(ign in domain for ign in IGNORE_FOR_WEBSITE):
                data["Website"] = href

    return {k: _clean(v) for k, v in data.items() if v}


def extract_data_kuwait(page, url: str) -> Dict[str, str]:
    """Kuwait-specific extraction — different DOM structure from ArabLocal.

    Args:
        page: Scrapling response object.
        url: The business page URL.

    Returns:
        Dict of extracted fields.
    """
    text_nodes = page.css("body *::text").getall()
    full_text = "\n".join(t.strip() for t in text_nodes if t.strip())

    data: Dict[str, str] = {}

    # Name
    h2 = page.css("h2::text").get("").strip()
    if h2:
        h2 = re.sub(r"\s*\(\s*(?:Open|Closed)\s*\)\s*$", "", h2, flags=re.I).strip()
    if not h2 or len(h2) <= 2:
        title = page.css("title::text").get("").strip()
        h2 = re.sub(r"\s*[|\-\u2013]\s*(?:Kuwait).*$", "", title, flags=re.I).strip()
    data["Name"] = h2

    # Phone
    phones_raw = []
    tel_links = page.css('a[href^="tel:"]::attr(href)').getall()
    for t in tel_links:
        num = t.replace("tel:", "").strip()
        if num:
            phones_raw.append(num)

    if not phones_raw:
        phones_raw = re.findall(r"\+965[\s-]?\d{7,9}(?!\d)", full_text)
        if not phones_raw:
            phones_raw = re.findall(r"\+965\s?\d{1,3}[\s-]\d{4,8}(?!\d)", full_text)

    seen_p: set = set()
    cleaned_phones = []
    for p in phones_raw:
        c = re.sub(r"\s+", " ", p).strip()
        if c not in seen_p:
            seen_p.add(c)
            cleaned_phones.append(c)
    for i, phone in enumerate(cleaned_phones[:3], 1):
        data[f"Phone_{i}"] = phone

    # WhatsApp
    wa_links = page.css('a[href*="wa.me"]::attr(href)').getall()
    for wa in wa_links:
        m_wa = re.search(r'wa\.me/(\d+)', wa)
        if m_wa:
            data["WhatsApp"] = f"+{m_wa.group(1)}"
            break

    # Email
    m = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", full_text)
    if m:
        email = m.group(0)
        if "." in email.split("@")[1]:
            data["Email"] = email

    # Address
    gov_link = page.css('a[href*="/business/governorates/"]')
    gov_name = ""
    if gov_link:
        gov_name = gov_link[0].css("::text").get("").strip()
        data["Governorate"] = gov_name

    if gov_name:
        name_idx = full_text.find(data.get("Name", ""))
        gov_idx = full_text.find(gov_name)
        if name_idx >= 0 and gov_idx > name_idx:
            addr_block = full_text[name_idx + len(data.get("Name", "")):gov_idx].strip()
            addr_block = re.sub(r"^\s*\(\s*(?:Open|Closed)\s*\)\s*", "", addr_block)
            addr_block = re.sub(r"\s+", " ", addr_block).strip().strip(",").strip()
            if addr_block and len(addr_block) > 3:
                data["Location"] = addr_block

    data["Country"] = "Kuwait"

    # About / Description
    desc_parts = []
    in_desc = False
    for node in text_nodes:
        t = node.strip()
        if t == "Description":
            in_desc = True
            continue
        if in_desc:
            if t in ("Categories :", "Tags :", "Gallery", "Services",
                     "Menu", "Regular Working Hours", "Products Services"):
                break
            if t and len(t) > 2:
                desc_parts.append(t)
    about = " ".join(desc_parts).strip()
    if about:
        about = re.sub(r"\s+", " ", about).strip()
        if len(about) > 500:
            about = about[:497] + "..."
        if len(about) > 10:
            data["About"] = about

    # Website & Social links
    all_links = page.css('a[href]')
    for a in all_links:
        href = (a.css("::attr(href)").get("") or "").strip()
        if not href or not href.startswith("http"):
            continue
        domain = urlparse(href).netloc.lower()
        if any(acct in href for acct in SITE_SOCIAL_BLACKLIST):
            continue

        is_social = False
        for soc_domain, platform in SOCIAL_DOMAINS.items():
            if soc_domain in domain:
                if platform not in data:
                    data[platform] = href
                is_social = True
                break

        if not is_social and "Website" not in data:
            if not any(ign in domain for ign in IGNORE_FOR_WEBSITE):
                data["Website"] = href

    return {k: _clean(v) for k, v in data.items() if v}


# ─── Extraction Helpers ──────────────────────────────────────────────────────

def extract_name(page) -> str:
    """Extract business name from page using multiple fallback strategies."""
    for h3 in page.css("h3"):
        cls = h3.attrib.get("class", "")
        if "font_size_22" in cls:
            val = h3.css("::text").get("").strip()
            if val and len(val) > 2:
                return val
    h1 = page.css("h1::text").get("").strip()
    if h1 and len(h1) > 2:
        return h1
    title = page.css("title::text").get("").strip()
    if title:
        title = re.sub(
            r"\s*[|\-\u2013]\s*(?:UAE|Kuwait|Saudi|Bahrain|Qatar|Oman).*$",
            "", title, flags=re.I
        ).strip()
        if title and len(title) > 2:
            return title
    return ""


def _extract_labelled(text: str, label: str) -> str:
    """Extract a value following a label like 'Address:', 'Area:', etc."""
    labels = r"Address|Area|Governorate|Country|Landmark|P\.?O\.?"
    pattern = rf"{label}\s*:\s*(.+?)(?=(?:{labels})\s*:|[\n])"
    m = re.search(pattern, text, re.IGNORECASE)
    if m:
        val = re.sub(r"\s+", " ", m.group(1)).strip()
        if len(val) > 200:
            val = val[:200].rsplit(" ", 1)[0]
        return val
    return ""


def _clean(text) -> str:
    """Clean extracted text: strip HTML tags, control chars, normalize whitespace."""
    if not text:
        return ""
    text = str(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
    return re.sub(r"\s+", " ", text).strip()
