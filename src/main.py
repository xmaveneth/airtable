#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, argparse, urllib.parse, requests
from typing import Dict, Any, List, Iterable, Optional, Set

from dotenv import load_dotenv

load_dotenv()

# ================== CONFIG via env ==================
AIRTABLE_TOKEN   = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")

# РЕКОМЕНДУЮ указывать ID таблиц (tbl...), но можно и имена
TABLE_A = os.getenv("TABLE_A", "tbl640VHuvsjlsG8d")   # Startups
TABLE_B = os.getenv("TABLE_B", "tblyx3dFe2OpJRdMf")   # Chris | Startups Data

KEY_A = os.getenv("KEY_A", "Company name")
KEY_B = os.getenv("KEY_B", "Company Name")

# Что переносим из B -> в A
FIELDS_TO_COPY = [
    "Description",
    "Employees",
    "Location",
    "Money Raised",
    "URL",
    "Vertical",                # -> drawdown_solutions (multi-select)
    "Email Reasoning",         # -> email_reasoning (long text)
    "Financials Reasoning",    # -> financials_reasoning (long text)
    "CEO Email"
]

# Соответствия имён (B -> A)
FIELD_MAP: Dict[str, str] = {
    "Description": "description",
    "Employees": "employees_count",
    "Location": "location",
    "Money Raised": "total_funding",
    "URL": "website",
    "Vertical": "drawdown_solutions",          # <— ключевое изменение
    "Email Reasoning": "email_reasoning",
    "Financials Reasoning": "financials_reasoning",
    "CEO Email": "ceo_email",
}

# Поля-назначения, которые пишем как строки
FORCE_STRING_FOR: Set[str] = {
    "employees_count", "description", "location", "total_funding", "website",
    "email_reasoning", "financials_reasoning",
}

# В A это мультиселект
MULTI_SELECT_DEST: Set[str] = {"drawdown_solutions"}

# Глобально накопим неизвестные опции мультиселекта
UNKNOWN_DRAW_OPTIONS: Set[str] = set()

# ================== Helpers ==================
def api_root() -> str:
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"

def headers() -> Dict[str, str]:
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

def chunks(a: List[Any], n: int):
    for i in range(0, len(a), n):
        yield a[i:i+n]

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

def to_multi_select(val) -> Optional[List[str]]:
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

def get_allowed_multiselect_options(table: str, field_name: str) -> Optional[set]:
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

def list_all(table: str) -> List[Dict[str,Any]]:
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

def batch_delete(table: str, ids: List[str], dry=False):
    for part in chunks(ids, 10):
        if dry: print(f"[DRY] DELETE {table}: {len(part)}"); continue
        url = f"{api_root()}/{urllib.parse.quote(table)}"
        params = [("records[]", rid) for rid in part]
        r = retry_request("DELETE", url, params=params)
        if not r.ok: raise RuntimeError(f"DELETE {table} -> {r.status_code} {r.text}")
        time.sleep(0.2)

# ================== Main ==================
def main(dry_run=False):
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
        raise SystemExit("❌ Укажи AIRTABLE_TOKEN и AIRTABLE_BASE_ID.")

    print(f"→ Загружаю A ({TABLE_A}) ...")
    A = list_all(TABLE_A)
    print(f"  Получено из A: {len(A)}")

    print(f"→ Загружаю B ({TABLE_B}) ...")
    B = list_all(TABLE_B)
    print(f"  Получено из B: {len(B)}")

    # допустимые опции для drawdown_solutions
    allowed_draw_opts = get_allowed_multiselect_options(TABLE_A, "drawdown_solutions")
    if not allowed_draw_opts:
        print("⚠️ Не удалось определить разрешённые опции drawdown_solutions — значения Vertical будут пропущены, чтобы не словить 422.")

    # индекс по ключу в A
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

        payload: Dict[str,Any] = {}
        for src in FIELDS_TO_COPY:
            if src not in fb or fb[src] in (None, ""):
                continue
            dst = FIELD_MAP.get(src, src)

            if dst in MULTI_SELECT_DEST:
                base_val = unwrap_value(fb[src])          # строка (или список) из Vertical
                vals = to_multi_select(base_val) or []
                vals = [sanitize_option_label(v) for v in vals if v and str(v).strip()]
                if allowed_draw_opts:
                    allowed = [v for v in vals if v in allowed_draw_opts]
                    unknown = [v for v in vals if v not in allowed_draw_opts]
                    if unknown:
                        UNKNOWN_DRAW_OPTIONS.update(unknown)
                    if not allowed:
                        continue  # всё неизвестно — пропускаем, чтобы не было 422
                    val = allowed
                else:
                    continue  # нет списка допустимых — безопаснее пропустить
            else:
                val = unwrap_value(fb[src])
                if val is None:
                    continue
                if dst in FORCE_STRING_FOR:
                    val = str(val)

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

    # дедуп по ключу
    print("→ Дедуп в A ...")
    A2 = list_all(TABLE_A)
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

    # Отчёт по неизвестным опциям мультиселекта
    if UNKNOWN_DRAW_OPTIONS:
        try:
            with open("missing_drawdown_options.txt", "w", encoding="utf-8") as f:
                for x in sorted(UNKNOWN_DRAW_OPTIONS):
                    f.write(f"{x}\n")
            print(f"⚠️ В поле drawdown_solutions отсутствуют {len(UNKNOWN_DRAW_OPTIONS)} опций (из Vertical).")
            print("   Список сохранён в missing_drawdown_options.txt — добавьте их в настройках поля и перезапустите скрипт.")
        except Exception:
            print(f"⚠️ Отсутствующие опции (первые 20): {list(sorted(UNKNOWN_DRAW_OPTIONS))[:20]}")

    print("✅ Готово")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Airtable merge + enrich + dedupe (Vertical -> drawdown_solutions)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    main(dry_run=args.dry_run)
