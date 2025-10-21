import time
import urllib.parse
import requests
from typing import Any

from src.main import AIRTABLE_BASE_ID, AIRTABLE_TOKEN


def api_root() -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"

def headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type": "application/json"}

def retry_request(method, url, **kw):
    retries, base_wait = 6, 0.6
    for attempt in range(1, retries + 1):
        r = requests.request(method, url, headers=headers(), **kw)
        if r.status_code in (429,) or r.status_code >= 500:
            wait = float(r.headers.get("Retry-After", base_wait * attempt))
            time.sleep(wait); continue
        return r
    return r

def chunks(a: list[Any], n: int):
    for i in range(0, len(a), n):
        yield a[i:i+n]

def normalize_key(v: Any) -> str:
    if v is None: return ""
    s = str(v).strip().lower()
    if s.startswith("http://") or s.startswith("https://"): s = s.split("://",1)[1]
    if s.startswith("www."): s = s[4:]
    while s.endswith("/"): s = s[:-1]
    return s

def count_filled(fields: dict[str,Any]) -> int:
    c = 0
    for v in fields.values():
        if v is None or v == "": continue
        if isinstance(v,(list,dict)):
            if len(v) > 0: c += 1
        else:
            c += 1
    return c

def unwrap_value(v):
    """single select -> name; multi -> 'name1, name2'; списки/объекты -> строка"""
    if v is None: return None
    if isinstance(v, dict) and "name" in v:  # single select
        return v["name"]
    if isinstance(v, list):
        if not v: return None
        if isinstance(v[0], dict) and "name" in v[0]:
            return ", ".join(str(x.get("name","")) for x in v if isinstance(x, dict))
        return ", ".join(str(x) for x in v)
    return v

def to_multi_select(val) -> list[str] | None:
    """Ожидаемый форм-фактор для multipleSelects: список строк (имен опций)."""
    if val is None or val == "":
        return None
    if isinstance(val, list):
        return [str(x) for x in val if str(x).strip() != ""]
    # поддержим строки с разделителями (, ; / |)
    s = str(val)
    for sep in [",", ";", "/", "|"]:
        if sep in s:
            return [p.strip() for p in s.split(sep) if p.strip()]
    return [s]

def sanitize_option_label(s: str) -> str:
    s = str(s).strip()
    # убираем лишние кавычки вида ""Something""
    if s.startswith('"') and s.endswith('"'): s = s[1:-1].strip()
    if s.startswith("'") and s.endswith("'"): s = s[1:-1].strip()
    return s

def get_allowed_multiselect_options(table: str, field_name: str) -> set | None:
    """
    Пытаемся получить список допустимых опций для мультиселекта:
    1) через meta API (нужен scope schema.bases:read);
    2) если нет прав — соберём из уже существующих данных в A.
    """
    # (1) meta API
    url = f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"
    r = retry_request("GET", url)
    if r.ok:
        try:
            for t in r.json().get("tables", []):
                if t["id"] == table or t["name"] == table:
                    for f in t["fields"]:
                        if f["name"] == field_name and f.get("type") in ("multipleSelects", "singleSelect"):
                            choices = f.get("options", {}).get("choices", [])
                            return {c["name"] for c in choices if "name" in c}
        except Exception:
            pass
    # (2) fallback из данных
    allowed = set()
    try:
        for rec in list_all(table):
            vals = rec.get("fields", {}).get(field_name)
            if isinstance(vals, list):
                for v in vals:
                    if isinstance(v, str):
                        allowed.add(v)
                    elif isinstance(v, dict) and "name" in v:
                        allowed.add(v["name"])
    except Exception:
        pass
    return allowed or None

def list_all(table: str) -> list[dict[str,Any]]:
    out, offset = [], None
    while True:
        qs = []
        if offset: qs.append(("offset", offset))
        url = f"{api_root()}/{urllib.parse.quote(table)}"
        r = retry_request("GET", url, params=qs)
        if not r.ok:
            raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
        j = r.json()
        out += j.get("records", [])
        offset = j.get("offset")
        if not offset: break
    return out

def batch_create(table: str, recs: list[dict[str,Any]], dry=False):
    for part in chunks(recs, 10):
        if dry: print(f"[DRY] POST {table}: {len(part)}"); continue
        r = retry_request("POST", f"{api_root()}/{urllib.parse.quote(table)}", json={"records": part})
        if not r.ok: raise RuntimeError(f"POST {table} -> {r.status_code} {r.text}")
        time.sleep(0.2)

def batch_update(table: str, recs: list[dict[str,Any]], dry=False):
    for part in chunks(recs, 10):
        if dry: print(f"[DRY] PATCH {table}: {len(part)}"); continue
        r = retry_request("PATCH", f"{api_root()}/{urllib.parse.quote(table)}", json={"records": part})
        if not r.ok: raise RuntimeError(f"PATCH {table} -> {r.status_code} {r.text}")
        time.sleep(0.2)

def batch_delete(table: str, ids: list[str], dry=False):
    for part in chunks(ids, 10):
        if dry: print(f"[DRY] DELETE {table}: {len(part)}"); continue
        url = f"{api_root()}/{urllib.parse.quote(table)}"
        params = [("records[]", rid) for rid in part]
        r = retry_request("DELETE", url, params=params)
        if not r.ok: raise RuntimeError(f"DELETE {table} -> {r.status_code} {r.text}")
        time.sleep(0.2)
