import json
import argparse
from typing import Any, Dict, Tuple, Optional

from dotenv import load_dotenv
load_dotenv()


from src.helpers import (
    list_all, batch_update, normalize_key,
)
from src.main import (
    AIRTABLE_BASE_ID, AIRTABLE_TOKEN,
    TABLE_A, KEY_A
)

# Поля к заполеннию
TARGET_FIELDS = (
    "total_funding",
    "employees_count",
    "location",
    "linkedin_url",
    "latest funding type",
)

def is_empty(v: Any) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "") or (isinstance(v, list) and len(v) == 0)

def richness_from_node_attr(attr: Dict[str, Any]) -> int:
    """Сколько целевых полей потенциально можно взять из узла."""
    score = 0
    if attr.get("Total Funding") not in (None, ""): score += 1
    if attr.get("Company Size") not in (None, ""): score += 1
    if attr.get("HQ City") or (isinstance(attr.get("Geo Mentions"), list) and attr.get("Geo Mentions")): score += 1
    if attr.get("LinkedIn"): score += 1
    if attr.get("Last Funding Type"): score += 1
    return score

def extract_values_from_node(attr: Dict[str, Any]) -> Dict[str, Any]:
    """Достаём значения для полей A"""
    total_funding = attr.get("Total Funding")
    total_funding = str(total_funding) if total_funding not in (None, "") else None

    employees_count = attr.get("Company Size")
    employees_count = str(employees_count) if employees_count not in (None, "") else None

    location = None
    if attr.get("HQ City"):
        location = str(attr.get("HQ City")).strip()
    else:
        gm = attr.get("Geo Mentions")
        if isinstance(gm, list) and gm:
            location = str(gm[0]).strip()

    linkedin_url = attr.get("LinkedIn")
    linkedin_url = str(linkedin_url).strip() if linkedin_url not in (None, "") else None

    latest_funding_type = attr.get("Last Funding Type")
    if isinstance(latest_funding_type, str):
        latest_funding_type = latest_funding_type.strip().strip('"').strip("'")
        if latest_funding_type == "":
            latest_funding_type = None

    return {
        "total_funding": total_funding,
        "employees_count": employees_count,
        "location": location,
        "linkedin_url": linkedin_url,
        "latest funding type": latest_funding_type,
    }

def build_nodes_indexes(nodes: Dict[str, Any]) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    """
    Строим два индекса по nodes.json:
    - по нормализованному названию компании (Name)
    - по нормализованному домену сайта (Website)
    Если есть дубликаты, берём тот узел, который даёт больше целевых полей.
    """
    by_name: Dict[str, Dict] = {}
    by_site: Dict[str, Dict] = {}

    datapoints = nodes.get("datapoints", [])
    for dp in datapoints:
        attr = (dp or {}).get("attr", {})
        name = attr.get("Name")
        site = attr.get("Website")

        r = richness_from_node_attr(attr)

        if name:
            kn = normalize_key(name)
            if kn and (kn not in by_name or richness_from_node_attr(by_name[kn]["attr"]) < r):
                by_name[kn] = dp

        if site:
            ks = normalize_key(site)
            if ks and (ks not in by_site or richness_from_node_attr(by_site[ks]["attr"]) < r):
                by_site[ks] = dp

    return by_name, by_site

def choose_best_node(candidate_a: Optional[Dict], candidate_b: Optional[Dict]) -> Optional[Dict]:
    """Выбираем между 2 кандидатами (по сайту и по имени) наиболее 'богатый'."""
    if candidate_a and not candidate_b:
        return candidate_a
    if candidate_b and not candidate_a:
        return candidate_b
    if not candidate_a and not candidate_b:
        return None
    ra = richness_from_node_attr(candidate_a["attr"])
    rb = richness_from_node_attr(candidate_b["attr"])
    return candidate_a if ra >= rb else candidate_b

def main(nodes_path: str, dry_run: bool = False):
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
        raise SystemExit("Укажи AIRTABLE_TOKEN и AIRTABLE_BASE_ID.")

    if not TABLE_A or not KEY_A:
        raise SystemExit("Укажи TABLE_A и KEY_A.")

    with open(nodes_path, "r", encoding="utf-8") as f:
        nodes = json.load(f)

    by_name, by_site = build_nodes_indexes(nodes)
    print(f"Индекс по имени: {len(by_name)}, по сайту: {len(by_site)}")

    print(f"→ Загружаю A ({TABLE_A}) ...")
    A = list_all(TABLE_A)
    print(f"  Получено из A: {len(A)}")

    to_update = []
    report_rows = []

    # Фильтруем записи в A, где есть хотя бы одно пустое поле из TARGET_FIELDS
    need_fill = []
    for rec in A:
        fields = rec.get("fields", {})
        if any(is_empty(fields.get(f)) for f in TARGET_FIELDS):
            need_fill.append(rec)

    print(f"Нужно дополнить записей: {len(need_fill)}")

    # ищем соответствие в json
    for rec in need_fill:
        rid = rec["id"]
        fa = rec.get("fields", {})
        key_raw = fa.get(KEY_A)
        site_a = fa.get("website")
        match_by_site = by_site.get(normalize_key(site_a)) if site_a else None

        match_by_site2 = None
        if key_raw and isinstance(key_raw, str) and key_raw.strip().lower().startswith(("http://", "https://", "www.")):
            match_by_site2 = by_site.get(normalize_key(key_raw))

        match_by_name = by_name.get(normalize_key(key_raw)) if key_raw else None

        node = choose_best_node(match_by_site or match_by_site2, match_by_name)
        if not node:
            report_rows.append({
                "record_id": rid,
                "company": key_raw,
                "matched": "no",
                "filled_fields": "",
            })
            continue

        attr = node.get("attr", {})
        extracted = extract_values_from_node(attr)

        patch = {}
        filled = []

        for dst_field, val in extracted.items():
            if is_empty(fa.get(dst_field)) and val not in (None, ""):
                patch[dst_field] = str(val)
                filled.append(dst_field)

        if patch:
            to_update.append({"id": rid, "fields": patch})

        report_rows.append({
            "record_id": rid,
            "company": key_raw,
            "matched": "yes",
            "filled_fields": ", ".join(filled) if filled else "",
        })

    print(f"К обновлению записей: {len(to_update)}")

    if to_update:
        batch_update(TABLE_A, to_update, dry=dry_run)

    try:
        import csv
        report_path = "nodes_fill_report.csv"
        with open(report_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["record_id", "company", "matched", "filled_fields"])
            w.writeheader()
            for row in report_rows:
                w.writerow(row)
        print(f"Отчёт: {report_path} (строк: {len(report_rows)})")
    except Exception as e:
        print(f"Не удалось записать отчёт CSV: {e}")

    print("Готово.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Enrich Airtable A from json (total_funding, employees_count, location, linkedin_url)")
    ap.add_argument("--nodes", required=True, help="Путь к json")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    main(args.nodes, dry_run=args.dry_run)
