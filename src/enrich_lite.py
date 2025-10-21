import os, time, argparse, json, csv, urllib.parse, tldextract, itertools
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from dateutil import parser as dtp
from datetime import datetime, timezone
from src.parsing_helpers import *

load_dotenv()

# ----------------- ENV / CONFIG -----------------
AIRTABLE_TOKEN   = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
TABLE_A          = os.getenv("TABLE_A",)  # ID/имя Startups

# Поля в Airtable
FIELD_COMPANY = "Company name"
FIELD_WEBSITE = "website"
FIELD_LOC     = "location"
FIELD_FUND    = "total_funding"
FIELD_EMP     = "employees_count"
FIELD_EMAIL   = "ceo_email"                  # если поля нет, скрипт пропустит
FIELD_EMAIL_R = "email_reasoning"
FIELD_FIN_R   = "financials_reasoning"

# Служебные (если нет в таблице — не страшно, скрипт пропустит)
FIELD_SRC   = "enrichment_sources"
FIELD_TS    = "last_enriched_at"
FIELD_STAT  = "enrichment_status"            # success / partial / skipped / error

USER_AGENT = "Mozilla/5.0 (compatible; StartupEnricher/1.0; +https://example.com/bot-info)"
REQ_TIMEOUT = 20
SLEEP_BETWEEN = 0.6   # секунды между запросами

# Сколько страниц максимум с домена смотреть (чтобы не краулить слишком глубоко)
MAX_PAGES_PER_SITE = 12

# Ключевые url-слова и паттерны для поиска инфы
CANDIDATE_SLUGS = [
    "about", "team", "people", "leadership", "contact", "contacts", "imprint", "impressum",
    "press", "news", "media", "blog", "stories", "updates", "company", "who-we-are", "careers"
]
FUNDING_KEYWORDS = [
    "raised", "funding", "financing", "investment", "invested", "seed round",
    "series a", "series b", "series c", "pre-seed", "angel round", "grant"
]
EMAIL_NEAR_TITLES = ["ceo", "chief executive", "founder", "co-founder", "owner", "managing director"]

# ----------------------------------------------------------------
#                     AIRTABLE HELPERS
# ----------------------------------------------------------------
def api_root() -> str: return f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"
def headers() -> dict[str,str]: return {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type":"application/json"}

def list_all(table: str, fields: Optional[list[str]] = None) -> list[dict[str, Any]]:
    out, offset = [], None
    params = []
    if fields:
        for f in fields: params.append(("fields[]", f))
    while True:
        qs = list(params)
        if offset: qs.append(("offset", offset))
        url = f"{api_root()}/{urllib.parse.quote(table)}"
        r = requests.get(url, headers=headers(), params=qs, timeout=REQ_TIMEOUT)
        if not r.ok:
            raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
        j = r.json()
        out += j.get("records", [])
        offset = j.get("offset")
        if not offset: break
    return out

def batch_update_safe(table: str, records: list[dict[str,Any]]):
    """
    Надёжный PATCH: на 422 UNKNOWN_FIELD_NAME вытаскиваем имя поля из сообщения,
    выкидываем его из всех записей и повторяем.
    """
    idx = 0
    while idx < len(records):
        part = records[idx: idx+10]
        url = f"{api_root()}/{urllib.parse.quote(table)}"
        r = requests.patch(url, headers=headers(), json={"records": part}, timeout=REQ_TIMEOUT)
        if r.ok:
            idx += 10
            time.sleep(0.2)
            continue
        if r.status_code == 422 and "UNKNOWN_FIELD_NAME" in r.text:
            m = re.search(r'Unknown field name:\s*"([^"]+)"', r.text)
            unknown = m.group(1) if m else None
            if unknown:
                for rec in records:
                    if "fields" in rec and unknown in rec["fields"]:
                        del rec["fields"][unknown]
                continue
        raise RuntimeError(f"PATCH {table} -> {r.status_code} {r.text}")

# ----------------------------------------------------------------
#                       EXTRACTORS
# ----------------------------------------------------------------
def discover_candidate_urls(base: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("#"): continue
        abs_url = urllib.parse.urljoin(base, href)
        if not abs_url.startswith(("http://", "https://")): continue
        if any(slug in abs_url.lower() for slug in CANDIDATE_SLUGS):
            links.append(abs_url)
    # уберём дубли, сохранив порядок
    seen, out = set(), []
    for u in links:
        if u not in seen:
            out.append(u); seen.add(u)
    return out[:MAX_PAGES_PER_SITE-1]  # -1 потому что главную тоже смотрим

def extract_location_from_jsonld(jsonlds: list[dict[str,Any]]) -> Optional[str]:
    for obj in jsonlds:
        t = obj.get("@type")
        if isinstance(t, list):
            types = [x.lower() for x in t]
        else:
            types = [str(t).lower()] if t else []
        if any(tt in ("organization","localbusiness","corp","corporation","ngo") for tt in types):
            addr = obj.get("address")
            if isinstance(addr, dict):
                loc = city_country_from_address(addr)
                if loc: return loc
    return None

def extract_employees_from_jsonld(jsonlds: list[dict[str,Any]]) -> Optional[str]:
    for obj in jsonlds:
        n = obj.get("numberOfEmployees")
        if n:
            rng = number_or_range(n)
            if rng: return rng
    return None

def count_team_cards(soup: BeautifulSoup) -> Optional[str]:
    cand = soup.select('[class*="team"], [class*="member"], [class*="person"], [class*="staff"], [id*="team"]')
    items = []
    for el in cand:
        if el.find(["img","figure"]) or el.find(re.compile("^h[1-6]$")):
            items.append(el)
    items = items[:200]
    if len(items) >= 2:
        return number_or_range(len(items))
    return None

def extract_emails(soup: BeautifulSoup) -> list[str]:
    emails = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("mailto:"):
            em = href.split("mailto:",1)[1].split("?")[0]
            if EMAIL_RE.match(em): emails.add(em)
    for text in soup.stripped_strings:
        m = EMAIL_RE.search(text)
        if m: emails.add(m.group(0))
    return list(emails)

def find_ceo_email(soup: BeautifulSoup, domain: str) -> Optional[str]:
    page_text = soup.get_text(" ", strip=True).lower()
    if any(k in page_text for k in EMAIL_NEAR_TITLES):
        emails = extract_emails(soup)
        emails = [e for e in emails if e.lower().endswith("@"+domain)]
        if emails:
            return emails[0]
    return None

def extract_funding_from_article(html: str, url: str) -> Optional[tuple[str,str]]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    if not (RAISED_RE.search(text) or ROUND_RE.search(text)):
        return None
    m = MONEY_RE.search(text)
    if not m: return None
    amount = normalize_money(m.groupdict())
    dt = None
    for attr in ["article:published_time","og:published_time","article:modified_time","og:updated_time","date"]:
        tag = soup.find("meta", attrs={"property": attr}) or soup.find("meta", attrs={"name": attr})
        if tag and tag.get("content"):
            try:
                dt = dtp.parse(tag["content"]).date().isoformat()
                break
            except Exception:
                pass
    if not dt:
        t = soup.find("time")
        if t and t.get("datetime"):
            try:
                dt = dtp.parse(t["datetime"]).date().isoformat()
            except Exception:
                pass
    reasoning = f"{amount} via site article {('(' + dt + ')') if dt else ''} {url}"
    return amount, reasoning

# ----------------------------------------------------------------
#                      ENRICH ONE COMPANY
# ----------------------------------------------------------------
def enrich_from_site(website: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    sources: list[str] = []
    if not website:
        return out

    url0 = website if website.startswith("http") else "https://" + website
    domain = tldextract.extract(url0).registered_domain
    if not domain:
        return out
    home = f"https://{domain}/"

    rp = robotparser.RobotFileParser()
    try:
        rp.set_url(f"https://{domain}/robots.txt"); rp.read()
    except Exception:
        pass

    sess = make_session()

    # 1) Главная
    if can_fetch(rp, home):
        html, base = fetch(sess, home); time.sleep(SLEEP_BETWEEN)
    else:
        html, base = None, None

    candidates = []
    if html and base:
        jsonlds = extract_jsonld(html, base)
        loc = extract_location_from_jsonld(jsonlds)
        if loc:
            out.setdefault("location", loc)
            sources.append("site:jsonld")
        emp = extract_employees_from_jsonld(jsonlds)
        if emp:
            out.setdefault("employees_count", emp)
            sources.append("site:jsonld")

        candidates = discover_candidate_urls(base, html)

        soup = BeautifulSoup(html, "lxml")
        emp2 = count_team_cards(soup)
        if emp2 and "employees_count" not in out:
            out["employees_count"] = emp2
            sources.append("site:team-count")

        ceo = find_ceo_email(soup, domain)
        if ceo:
            out["ceo_email"] = ceo
            out["email_reasoning"] = f"Found mailto near CEO/Founder on homepage {home}"
            sources.append("site:homepage-mailto")

        if "location" not in out:
            footer = soup.find("footer")
            if footer:
                txt = footer.get_text(" ", strip=True)
                m = re.search(r"([A-Z][A-Za-z\-\s]+),\s*([A-Z][A-Za-z\-\s]+)$", txt)
                if m:
                    out["location"] = f"{m.group(1).strip()}, {m.group(2).strip()}"
                    sources.append("site:footer")

    # 2) Страницы-кандидаты
    for url in itertools.islice(candidates, 0, MAX_PAGES_PER_SITE-1):
        if not can_fetch(rp, url): continue
        html, base = fetch(sess, url); time.sleep(SLEEP_BETWEEN)
        if not html or not base: continue
        soup = BeautifulSoup(html, "lxml")
        jsonlds = extract_jsonld(html, base)

        if "location" not in out:
            loc = extract_location_from_jsonld(jsonlds)
            if loc:
                out["location"] = loc

        if "employees_count" not in out:
            emp = extract_employees_from_jsonld(jsonlds) or count_team_cards(soup)
            if emp:
                out["employees_count"] = emp

        if FIELD_EMAIL and "ceo_email" not in out:
            ceo = find_ceo_email(soup, domain)
            if not ceo and any(sl in url.lower() for sl in ["team","people","leadership"]):
                emails = [e for e in extract_emails(soup) if e.lower().endswith("@"+domain)]
                if len(emails) == 1:
                    ceo = emails[0]
            if ceo:
                out["ceo_email"] = ceo
                out.setdefault("email_reasoning", f"Found corporate email on {url}")

        if "total_funding" not in out and any(k in url.lower() for k in ["news","press","blog","stories","updates","media"]):
            hit = extract_funding_from_article(html, url)
            if hit:
                amount, finr = hit
                out["total_funding"] = amount
                out.setdefault("financials_reasoning", finr)

        if all(k in out for k in ["location","employees_count","total_funding","ceo_email"]):
            break

    if sources and FIELD_SRC:
        out[FIELD_SRC] = "\n".join(sorted(set(sources)))

    if "total_funding" in out and "financials_reasoning" not in out:
        out["financials_reasoning"] = f"{out['total_funding']} (from site)"
    if "ceo_email" in out and "email_reasoning" not in out:
        out["email_reasoning"] = f"Found mailto on site"

    return out

# ----------------------------------------------------------------
#                         MAIN LOGIC
# ----------------------------------------------------------------
def main(limit: int, dry_run: bool):
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
        raise SystemExit("Set AIRTABLE_TOKEN and AIRTABLE_BASE_ID")

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_csv = f"enrichment_report_{run_ts}.csv"
    report_jsonl = f"enrichment_report_{run_ts}.jsonl"

    target_fields = [FIELD_LOC, FIELD_FUND, FIELD_EMP, FIELD_EMAIL, FIELD_EMAIL_R, FIELD_FIN_R]
    need_fields = [FIELD_COMPANY, FIELD_WEBSITE] + target_fields + [FIELD_SRC, FIELD_TS, FIELD_STAT]
    recs = list_all(TABLE_A, fields=need_fields)
    print(f"→ Loaded records from A: {len(recs)}")

    # кандидаты с пустыми целевыми полями
    targets = []
    for r in recs:
        f = r.get("fields", {})
        if any(not f.get(x) for x in target_fields):
            if f.get(FIELD_WEBSITE):
                targets.append(r)
    print(f"→ To enrich: {len(targets)} (have website)")

    updates: list[dict[str,Any]] = []
    report_rows: list[dict[str, Any]] = []
    skipped = 0

    field_insert_counters = {k: 0 for k in target_fields}

    for r in targets[:limit]:
        f = r.get("fields", {})
        site = f.get(FIELD_WEBSITE)
        name = f.get(FIELD_COMPANY)
        rid = r["id"]

        try:
            found = enrich_from_site(site)
        except Exception as e:
            found = {}

        patch: dict[str,Any] = {}
        inserted_fields: list[str] = []

        # пишем только в пустые
        for key in [FIELD_LOC, FIELD_FUND, FIELD_EMP, FIELD_EMAIL, FIELD_EMAIL_R, FIELD_FIN_R, FIELD_SRC]:
            if not key: continue
            if key in found:
                cur = f.get(key)
                empty = (cur is None) or (cur == "") or (isinstance(cur, list) and len(cur) == 0)
                if empty:
                    patch[key] = found[key]
                    if key in field_insert_counters:
                        field_insert_counters[key] += 1
                    if key in target_fields:
                        inserted_fields.append(key)

        if patch:
            patch.setdefault(FIELD_TS, datetime.now(timezone.utc).isoformat())
            patch.setdefault(FIELD_STAT, "partial" if len(inserted_fields) < 3 else "success")
            updates.append({"id": rid, "fields": patch})

            # строка отчёта (только то, что реально вставляется)
            row = {
                "record_id": rid,
                "company": name or "",
                "website": site or "",
                "inserted_fields": ", ".join(inserted_fields) if inserted_fields else "",
                FIELD_LOC: patch.get(FIELD_LOC, ""),
                FIELD_EMP: patch.get(FIELD_EMP, ""),
                FIELD_FUND: patch.get(FIELD_FUND, ""),
                FIELD_EMAIL: patch.get(FIELD_EMAIL, ""),
                FIELD_EMAIL_R: patch.get(FIELD_EMAIL_R, ""),
                FIELD_FIN_R: patch.get(FIELD_FIN_R, "")
            }
            report_rows.append(row)
        else:
            skipped += 1

        time.sleep(0.2)  # общий rate-limit

    print(f"→ Will update: {len(updates)} | skipped (nothing new): {skipped}")

    # Печатаем превью отчёта (до отправки)
    preview = report_rows[:5]
    if preview:
        print("\nPreview of inserted data (first 5):")
        for row in preview:
            print(f"- {row['company']} | fields: {row['inserted_fields']} | website: {row['website']}")

    # Отправляем изменени
    if not dry_run and updates:
        batch_update_safe(TABLE_A, updates)
        print("Updated in Airtable.")
    elif dry_run:
        print("DRY-RUN only. No changes sent.")

    #Сохраняем отчёт
    if report_rows:
        # CSV
        csv_fields = ["record_id","company","website","inserted_fields",FIELD_LOC,FIELD_EMP,FIELD_FUND,FIELD_EMAIL,FIELD_EMAIL_R,FIELD_FIN_R]
        try:
            with open(report_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=csv_fields)
                w.writeheader()
                for row in report_rows:
                    w.writerow(row)
            print(f"CSV report saved: {report_csv}")
        except Exception as e:
            print(f"Failed to write CSV report: {e}")

        # JSONL
        try:
            with open(report_jsonl, "w", encoding="utf-8") as f:
                for row in report_rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
            print(f"JSONL report saved: {report_jsonl}")
        except Exception as e:
            print(f"Failed to write JSONL report: {e}")

    # Финальная сводка
    print("\n==== SUMMARY ====")
    print(f"Updated records: {len(updates)}")
    for k in target_fields:
        print(f"- inserted {k}: {field_insert_counters[k]}")
    print(f"Skipped (nothing to insert): {skipped}")
    if report_rows:
        print(f"Report files: {report_csv} and {report_jsonl}")
    print("=================\n")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Lite enrichment from company websites (no paid APIs) with reporting")
    ap.add_argument("--limit", type=int, default=10, help="сколько компаний обрабатывать за один запуск")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    main(limit=args.limit, dry_run=args.dry_run)
