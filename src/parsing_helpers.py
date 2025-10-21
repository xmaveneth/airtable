import re, urllib.parse, requests
from typing import Any, Optional
from w3lib.html import get_base_url
import extruct
from urllib import robotparser

from src.enrich_lite import USER_AGENT, REQ_TIMEOUT


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})
    s.max_redirects = 5
    return s

def norm_domain(url: str | None) -> str | None:
    if not url: return None
    u = url.strip()
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "https://" + u
    try:
        p = urllib.parse.urlparse(u)
        host = p.netloc.lower()
        return host or None
    except Exception:
        return None

def can_fetch(robots: robotparser.RobotFileParser, url: str) -> bool:
    try:
        return robots.can_fetch(USER_AGENT, url)
    except Exception:
        return True

def fetch(session: requests.Session, url: str) -> tuple[Optional[str], Optional[str]]:
    try:
        r = session.get(url, timeout=REQ_TIMEOUT)
        if r.status_code >= 400:
            return None, None
        base = get_base_url(r.text, r.url)
        return r.text, base
    except Exception:
        return None, None

def extract_jsonld(html: str, base_url: str) -> list[dict[str, Any]]:
    try:
        data = extruct.extract(html, base_url=base_url, syntaxes=["json-ld"], errors="ignore")
        return data.get("json-ld", []) or []
    except Exception:
        return []

def text_or_none(x: Any) -> Optional[str]:
    s = str(x).strip() if x is not None else None
    return s or None

def city_country_from_address(addr_obj: dict[str, Any]) -> Optional[str]:
    city = addr_obj.get("addressLocality") or addr_obj.get("addressRegion")
    country = addr_obj.get("addressCountry")
    if isinstance(country, dict):
        country = country.get("name") or country.get("identifier")
    city = text_or_none(city)
    country = text_or_none(country)
    if city and country:
        return f"{city}, {country}"
    return country or city

def number_or_range(n: Any) -> Optional[str]:
    if n is None: return None
    try:
        val = int(str(n).strip())
        if val <= 10: return "1-10"
        if val <= 50: return "11-50"
        if val <= 200: return "51-200"
        if val <= 500: return "201-500"
        return "500+"
    except Exception:
        s = str(n).strip()
        return s or None

EMAIL_RE = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)

MONEY_RE = re.compile(
    r"(?P<cur>[$€£]|USD|EUR|GBP)\s?(?P<num>(\d{1,3}([,\s]\d{3})+|\d+)(\.\d+)?)\s*(?P<suf>m|bn|b|k|million|billion|thousand)?",
    re.I
)
ROUND_RE = re.compile(r"(pre-?seed|seed|series\s+[abcde]|angel|grant)", re.I)
RAISED_RE = re.compile(r"\b(raised|raises|raise|secured|closed|financing|funding|investment)\b", re.I)

def normalize_money(groups: dict[str,str]) -> str:
    cur = groups.get("cur") or ""
    num = groups.get("num") or ""
    suf = (groups.get("suf") or "").lower()
    return f"{cur}{num}{(' ' + suf) if suf else ''}"