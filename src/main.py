import os
import argparse
from src.helpers import *

from dotenv import load_dotenv

load_dotenv()

# ================== CONFIG via env ==================
AIRTABLE_TOKEN   = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")

# Ууказывать ID таблиц (tbl...)
TABLE_A = os.getenv("TABLE_A")   # Startups
TABLE_B = os.getenv("TABLE_B")   # Chris | Startups Data

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
FIELD_MAP: dict[str, str] = {
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
FORCE_STRING_FOR: set[str] = {
    "employees_count", "description", "location", "total_funding", "website",
    "email_reasoning", "financials_reasoning",
}

# В A это мультиселект
MULTI_SELECT_DEST: set[str] = {"drawdown_solutions"}

# Глобально накопим неизвестные опции мультиселекта
UNKNOWN_DRAW_OPTIONS: set[str] = set()

def main(dry_run=False):
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
        raise SystemExit("Укажи AIRTABLE_TOKEN и AIRTABLE_BASE_ID.")

    print(f"→ Загружаю A ({TABLE_A}) ...")
    A = list_all(TABLE_A)
    print(f"  Получено из A: {len(A)}")

    print(f"→ Загружаю B ({TABLE_B}) ...")
    B = list_all(TABLE_B)
    print(f"  Получено из B: {len(B)}")

    # допустимые опции для drawdown_solutions
    allowed_draw_opts = get_allowed_multiselect_options(TABLE_A, "drawdown_solutions")
    if not allowed_draw_opts:
        print("Не удалось определить разрешённые опции drawdown_solutions — значения Vertical будут пропущены, чтобы не словить 422.")

    # индекс по ключу в A
    by_key_a: dict[str, list[dict[str,Any]]] = {}
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

        payload: dict[str,Any] = {}
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

    print(f"Будет создано: {len(to_create)}, обновлено: {len(to_update)}")
    if to_create: batch_create(TABLE_A, to_create, dry=dry_run)
    if to_update: batch_update(TABLE_A, to_update, dry=dry_run)

    # дедуп по ключу
    print("Дедуп в A ...")
    A2 = list_all(TABLE_A)
    buckets: dict[str, list[dict[str,Any]]] = {}
    for r in A2:
        k = normalize_key(r.get("fields", {}).get(KEY_A))
        if not k: continue
        buckets.setdefault(k, []).append(r)

    to_del = []
    for k, arr in buckets.items():
        if len(arr) <= 1: continue
        arr_sorted = sorted(arr, key=lambda rec: count_filled(rec.get("fields", {})), reverse=True)
        to_del += [rec["id"] for rec in arr_sorted[1:]]

    print(f"Дубликатов к удалению: {len(to_del)}")
    if to_del: batch_delete(TABLE_A, to_del, dry=dry_run)

    # Отчёт по неизвестным опциям мультиселекта
    if UNKNOWN_DRAW_OPTIONS:
        try:
            with open("missing_drawdown_options.txt", "w", encoding="utf-8") as f:
                for x in sorted(UNKNOWN_DRAW_OPTIONS):
                    f.write(f"{x}\n")
            print(f"В поле drawdown_solutions отсутствуют {len(UNKNOWN_DRAW_OPTIONS)} опций (из Vertical).")
            print("Список сохранён в missing_drawdown_options.txt — добавьте их в настройках поля и перезапустите скрипт.")
        except Exception:
            print(f"Отсутствующие опции (первые 20): {list(sorted(UNKNOWN_DRAW_OPTIONS))[:20]}")

    print("Готово")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Airtable merge + enrich + dedupe (Vertical -> drawdown_solutions)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    main(dry_run=args.dry_run)
