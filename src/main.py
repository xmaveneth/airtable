#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, argparse, urllib.parse, requests
from typing import Dict, Any, List, Iterable, Optional

from dotenv import load_dotenv

load_dotenv()

AIRTABLE_TOKEN   = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")

# Можно указывать ИМЯ или ID (tbl...). РЕКОМЕНДУЮ: ID.
TABLE_A = os.getenv("TABLE_A", "Startups")
TABLE_B = os.getenv("TABLE_B", "Chris | Startups Data")

KEY_A = os.getenv("KEY_A", "Company name")
KEY_B = os.getenv("KEY_B", "Company Name")

# Что переносим из B -> в A (если в A пусто)
FIELDS_TO_COPY = ["Description", "Employees", "Location", "Money Raised", "URL"]

# Соответствия имён (B -> A)
FIELD_MAP = {
    "Description": "description",
    "Employees": "employees_count",
    "Location": "location",
    "Money Raised": "total_funding",
    "URL": "website",
}

def api_root() -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"

def headers() -> Dict[str,str]:
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


def unwrap_value(v):
  # single/multi select -> строка; списки/объекты -> строка
  if v is None:
    return None
  if isinstance(v, dict) and "name" in v:
    return v["name"]
  if isinstance(v, list):
    if not v:
      return None
    if isinstance(v[0], dict) and "name" in v[0]:
      return ", ".join(str(x.get("name", "")) for x in v if isinstance(x, dict))
    return ", ".join(str(x) for x in v)
  return v

def chunks(a: List[Any], n: int):
    for i in range(0, len(a), n): yield a[i:i+n]

def normalize_key(v: Any) -> str:
    if v is None: return ""
    s = str(v).strip().lower()
    if s.startswith("http://") or s.startswith("https://"): s = s.split("://",1)[1]
    if s.startswith("www."): s = s[4:]
    while s.endswith("/"): s = s[:-1]
    return s

def count_filled(fields: Dict[str,Any]) -> int:
    c = 0
    for v in fields.values():
        if v is None or v == "": continue
        if isinstance(v,(list,dict)): c += 1 if len(v)>0 else 0
        else: c += 1
    return c

def list_all(table: str, fields: Optional[List[str]] = None) -> List[Dict[str,Any]]:
    out, offset = [], None
    params = []
    if fields:
        for f in fields: params.append(("fields[]", f))
    while True:
        qs = list(params)
        if offset: qs.append(("offset", offset))
        url = f"{api_root()}/{urllib.parse.quote(table)}" + ("?"+urllib.parse.urlencode(qs) if qs else "")
        r = retry_request("GET", url)
        if not r.ok:
            raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
        j = r.json()
        out += j.get("records", [])
        offset = j.get("offset")
        if not offset: break
    return out

FORCE_STRING_FOR = {"employees_count", "description", "location", "total_funding", "website"}

def batch_create(table: str, recs: List[Dict[str,Any]], dry=False):
    for part in chunks(recs, 10):
        if dry: print(f"[DRY] POST {table}: {len(part)}"); continue
        r = retry_request("POST", f"{api_root()}/{urllib.parse.quote(table)}", json={"records": part})
        if not r.ok: raise RuntimeError(f"POST {table} -> {r.status_code} {r.text}")
        time.sleep(0.2)

def batch_update(table: str, recs: List[Dict[str,Any]], dry=False):
    for part in chunks(recs, 10):
        if dry: print(f"[DRY] PATCH {table}: {len(part)}"); continue
        r = retry_request("PATCH", f"{api_root()}/{urllib.parse.quote(table)}", json={"records": part})
        if not r.ok: raise RuntimeError(f"PATCH {table} -> {r.status_code} {r.text}")
        time.sleep(0.2)


def batch_delete(table: str, ids: list[str], dry: bool = False):
  for part in chunks(ids, 10):  # максимум 10 за запрос
    if dry:
      print(f"[DRY] DELETE {table}: {len(part)}")
      continue
    url = f"{api_root()}/{urllib.parse.quote(table)}"
    # передаём как query: records[]=rec1&records[]=rec2...
    params = [("records[]", rid) for rid in part]
    r = retry_request("DELETE", url, params=params)
    if not r.ok:
      raise RuntimeError(f"DELETE {table} -> {r.status_code} {r.text}")
    time.sleep(0.2)

def main(dry_run=False):
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
        raise SystemExit("❌ Укажи AIRTABLE_TOKEN и AIRTABLE_BASE_ID.")

    fields_a = list({KEY_A, *FIELD_MAP.values()})
    fields_b = list({KEY_B, *FIELDS_TO_COPY})

    print(f"→ Загружаю A ({TABLE_A}) ...")
    A = list_all(TABLE_A, fields=fields_a)
    print(f"  Получено из A: {len(A)}")

    print(f"→ Загружаю B ({TABLE_B}) ...")
    B = list_all(TABLE_B, fields=fields_b)
    print(f"  Получено из B: {len(B)}")

    by_key_a: Dict[str, List[Dict[str,Any]]] = {}
    for r in A:
        k = normalize_key(r.get("fields", {}).get(KEY_A))
        if not k: continue
        by_key_a.setdefault(k, []).append(r)

    to_create, to_update = [], []
    for rb in B:
        fb = rb.get("fields", {})
        key_b_raw = fb.get(KEY_B)
        k = normalize_key(key_b_raw)
        if not k: continue

        payload = {}
        for src in FIELDS_TO_COPY:
          if src in fb and fb[src] not in (None, ""):
              dst = FIELD_MAP.get(src, src)
              val = unwrap_value(fb[src])
              if val is None:
                  continue
              if dst in FORCE_STRING_FOR:
                  val = str(val)  # здесь ключевой фикс
              payload[dst] = val


        matches = by_key_a.get(k)
        if matches:
            tgt = matches[0]
            fa = tgt.get("fields", {})
            patch = {}
            for dst, v in payload.items():
                cur = fa.get(dst)
                empty = cur is None or cur == "" or (isinstance(cur, list) and len(cur) == 0)
                if empty:
                    patch[dst] = v
            if patch:
                to_update.append({"id": tgt["id"], "fields": patch})
        else:
            new_fields = {KEY_A: key_b_raw}
            new_fields.update(payload)
            to_create.append({"fields": new_fields})

    print(f"→ Будет создано: {len(to_create)}, обновлено: {len(to_update)}")
    if to_create: batch_create(TABLE_A, to_create, dry=dry_run)
    if to_update: batch_update(TABLE_A, to_update, dry=dry_run)

    print("→ Дедуп в A ...")
    A2 = list_all(TABLE_A, fields=fields_a)
    buckets: Dict[str, List[Dict[str,Any]]] = {}
    for r in A2:
        k = normalize_key(r.get("fields", {}).get(KEY_A))
        if not k: continue
        buckets.setdefault(k, []).append(r)

    to_del = []
    for k, arr in buckets.items():
        if len(arr) <= 1: continue
        arr_sorted = sorted(arr, key=lambda rec: count_filled(rec.get("fields", {})), reverse=True)
        to_del += [rec["id"] for rec in arr_sorted[1:]]

    print(f"→ Дубликатов к удалению: {len(to_del)}")
    if to_del: batch_delete(TABLE_A, to_del, dry=dry_run)
    print("✅ Готово")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    main(dry_run=args.dry_run)
