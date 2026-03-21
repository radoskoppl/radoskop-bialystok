#!/usr/bin/env python3
"""
Scraper interpelacji i zapytań radnych z BIP Białystok.

UWAGA: Uruchom lokalnie — sandbox Cowork blokuje domeny

Źródło: https://www.bip.bialystok.pl/

BIP Białystok zawiera interpelacje i zapytania radnych.
Struktura: /interpelacje/szukaj — lista z filtrowaniem po kadencji.

Użycie:
  python3 scrape_interpelacje.py [--output docs/interpelacje.json]
                                 [--kadencja IX]
                                 [--fetch-details]
                                 [--debug]
"""

import argparse
import json
import os
import re
import sys
import time

try:
    import requests
except ImportError:
    print("Wymagany moduł: pip install requests")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Wymagany moduł: pip install beautifulsoup4")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://www.bip.bialystok.pl"

# Białystok BIP structure — adjust based on actual site
KADENCJE = {
    "IX":   {"label": "IX kadencja (2024–2029)", "search_term": "IX"},
    "VIII": {"label": "VIII kadencja (2018–2024)", "search_term": "VIII"},
}

HEADERS = {
    "User-Agent": "Radoskop/1.0 (https://bialystok.radoskop.pl; kontakt@radoskop.pl)",
    "Accept": "text/html",
}

DELAY = 0.5
PER_PAGE = 25


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def fetch_interpelacje_page(session, page, kadencja_search_term, debug=False):
    """Pobiera stronę listy interpelacji z BIP Białystok."""
    # Try standard BIP search pattern
    url = f"{BASE_URL}/interpelacje"
    params = {
        "page": page,
    }

    if debug:
        print(f"  [DEBUG] GET {url} params={params}")

    try:
        resp = session.get(url, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        if debug:
            print(f"  [DEBUG] Błąd pobierania: {e}")
        return None


def parse_interpelacje_list(html, kadencja_name, debug=False):
    """Parsuje listę interpelacji z BIP.

    Szuka tablic z wierszami zawierającymi:
      - Przedmiot interpelacji (link)
      - Radny
      - Status odpowiedzi
    """
    if not html:
        return [], 1

    soup = BeautifulSoup(html, "html.parser")
    records = []

    # BIP Białystok typically uses tables or list items for interpelacje
    main = soup.find("main") or soup

    # Try to find tables with interpelacje
    tables = main.find_all("table")
    if not tables:
        # Try to find list items
        tables = main.find_all("div", class_=re.compile(r'interpelacja|list-item', re.I))

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            rows = table.find_all("div", class_=re.compile(r'row|item', re.I))

        if len(rows) < 1:
            continue

        # Process rows
        for row in rows:
            cells = row.find_all(["td", "div"])
            if len(cells) < 2:
                continue

            record = {
                "przedmiot": "",
                "radny": "",
                "status": "",
                "bip_url": "",
                "typ": "interpelacja",
                "kadencja": kadencja_name,
            }

            # Try to extract interpelacja data from cells
            row_text = row.get_text(separator=" | ")

            # Look for subject link
            a = row.find("a", href=True)
            if a:
                record["przedmiot"] = a.get_text(strip=True)
                href = a.get("href", "")
                if href.startswith("/"):
                    record["bip_url"] = BASE_URL + href
                elif href.startswith("http"):
                    record["bip_url"] = href

            # Try to extract councillor name (usually second column)
            if len(cells) >= 2:
                radny_text = cells[1].get_text(strip=True)
                if radny_text and "interpelacja" not in radny_text.lower():
                    record["radny"] = radny_text

            # Try to extract status (usually third column)
            if len(cells) >= 3:
                status_text = cells[2].get_text(strip=True)
                if status_text:
                    record["status"] = status_text

            if record.get("przedmiot"):
                records.append(record)

    # Detect pagination
    total_pages = 1
    for a in soup.find_all("a"):
        href = a.get("href", "")
        txt = a.get_text(strip=True)
        # Check for page numbers
        if re.match(r"^\d+$", txt):
            p = int(txt)
            if p > total_pages:
                total_pages = p
        # Check for "next" links
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            p = int(m.group(1))
            if p > total_pages:
                total_pages = p

    if debug:
        print(f"  [DEBUG] Parsed {len(records)} records, total_pages={total_pages}")

    return records, total_pages


def fetch_interpelacja_detail(session, bip_url, debug=False):
    """Pobiera szczegóły interpelacji z jej strony."""
    if not bip_url:
        return {}

    if debug:
        print(f"  [DEBUG] GET {bip_url}")

    try:
        resp = session.get(bip_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        detail = {}
        # Look for table with details
        for row in soup.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if not th or not td:
                continue

            label = th.get_text(strip=True).lower()
            val = td.get_text(strip=True)

            if "typ" in label:
                detail["typ_full"] = val
            elif "nr" in label or "numer" in label:
                detail["nr_sprawy"] = val
            elif "data" in label and "wplyw" in label:
                detail["data_wplywu"] = parse_date(val)
            elif "data" in label and "odpowied" in label:
                detail["data_odpowiedzi"] = parse_date(val)

        # Find attachment links
        attachments = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if "attachment" in href or "download" in href or "pdf" in href.lower():
                full_url = BASE_URL + href if href.startswith("/") else href
                attachments.append({"nazwa": text, "url": full_url})

                text_lower = text.lower()
                if "odpowied" in text_lower:
                    detail["odpowiedz_url"] = full_url
                elif not detail.get("tresc_url"):
                    detail["tresc_url"] = full_url

        if attachments:
            detail["zalaczniki"] = attachments

        return detail
    except Exception as e:
        if debug:
            print(f"  [DEBUG] Error fetching detail {bip_url}: {e}")
        return {}


def parse_date(raw):
    """Konwertuje datę na format YYYY-MM-DD."""
    if not raw:
        return ""
    raw = raw.strip()
    # DD.MM.YYYY or DD.MM.YYYY HH:MM
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    # YYYY-MM-DD
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return raw[:10]
    return raw


# ---------------------------------------------------------------------------
# Category classification
# ---------------------------------------------------------------------------

CATEGORIES = {
    "transport": ["transport", "komunikacj", "autobus", "tramwaj", "drog", "ulic", "rondo",
                  "chodnik", "przejści", "parkow", "rower", "ścieżk"],
    "infrastruktura": ["infrastru", "remont", "naprawa", "budow", "inwesty", "moderniz",
                       "oświetl", "kanalizacj", "wodociąg"],
    "bezpieczeństwo": ["bezpiecz", "straż", "policj", "monitoring", "kradzież", "wandal"],
    "edukacja": ["szkoł", "edukacj", "przedszkol", "żłob", "nauczyc"],
    "zdrowie": ["zdrow", "szpital", "leczni", "medyc", "lekarz"],
    "środowisko": ["środowisk", "zieleń", "drzew", "park", "recykl", "odpady"],
    "mieszkalnictwo": ["mieszka", "lokal", "czynsz", "wspólnot"],
    "kultura": ["kultur", "bibliotek", "muzeum", "teatr", "koncert"],
    "sport": ["sport", "boisko", "stadion", "basen"],
    "pomoc społeczna": ["społeczn", "pomoc", "senior", "niepełnospr"],
    "budżet": ["budżet", "finansow", "wydatk", "podatk"],
    "administracja": ["administrac", "urzęd", "procedur"],
}


def classify_category(przedmiot):
    """Klasyfikuje kategorię interpelacji."""
    if not przedmiot:
        return "inne"
    text = przedmiot.lower()
    for cat, keywords in CATEGORIES.items():
        for kw in keywords:
            if kw in text:
                return cat
    return "inne"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def scrape(kadencje, output_path, fetch_details=True, debug=False):
    """Główna funkcja scrapowania."""
    session = requests.Session()
    all_records = []

    for kad_name in kadencje:
        kad = KADENCJE.get(kad_name)
        if not kad:
            print(f"Nieznana kadencja: {kad_name}")
            continue

        print(f"\n=== {kad['label']} ===")

        page = 1
        total_pages = None
        kad_records = []

        while True:
            try:
                html = fetch_interpelacje_page(session, page, kad['search_term'], debug=debug)
                records, pages = parse_interpelacje_list(html, kad_name, debug=debug)
            except Exception as e:
                print(f"  BŁĄD na stronie {page}: {e}")
                break

            if total_pages is None:
                total_pages = max(pages, 1)
                print(f"  Łącznie stron: {total_pages}")

            kad_records.extend(records)

            if debug:
                print(f"  Strona {page}/{total_pages}: {len(records)} rekordów")
            elif page % 10 == 0:
                print(f"  Strona {page}/{total_pages}...")

            if not records or page >= total_pages:
                break

            page += 1
            time.sleep(DELAY)

        print(f"  Pobrano: {len(kad_records)} rekordów")

        # Optionally fetch details for each record
        if fetch_details:
            print(f"\n  Pobieram szczegóły ({len(kad_records)} rekordów)...")
            for i, rec in enumerate(kad_records):
                bip_url = rec.get("bip_url", "")
                if not bip_url:
                    continue
                detail = fetch_interpelacja_detail(session, bip_url, debug=debug)
                if detail:
                    rec.update({k: v for k, v in detail.items() if v})
                if (i + 1) % 50 == 0:
                    print(f"  Szczegóły: {i+1}/{len(kad_records)}")
                time.sleep(DELAY)

        all_records.extend(kad_records)

    # Classify categories and normalize fields
    for rec in all_records:
        rec["kategoria"] = classify_category(rec.get("przedmiot", ""))

        # Normalize status
        status = rec.get("status", "").lower()
        rec["odpowiedz_status"] = status

        # Ensure consistent output fields
        rec.setdefault("data_wplywu", "")
        rec.setdefault("data_odpowiedzi", "")
        rec.setdefault("tresc_url", "")
        rec.setdefault("odpowiedz_url", "")
        rec.setdefault("nr_sprawy", "")

    # Sort by newest first
    all_records.sort(
        key=lambda x: x.get("data_wplywu", "") or x.get("bip_url", ""),
        reverse=True,
    )

    # Stats
    interp = sum(1 for r in all_records if r.get("typ") == "interpelacja")
    zap = sum(1 for r in all_records if r.get("typ") == "zapytanie")
    answered = sum(1 for r in all_records if "udzielono" in r.get("odpowiedz_status", ""))
    print(f"\n=== Podsumowanie ===")
    print(f"Interpelacje: {interp}")
    print(f"Zapytania:    {zap}")
    print(f"Z odpowiedzią: {answered}")
    print(f"Razem:        {len(all_records)}")

    # Save
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    size_kb = os.path.getsize(output_path) / 1024
    print(f"\nZapisano: {output_path} ({size_kb:.1f} KB)")
    print(f"Gotowe: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Scraper interpelacji i zapytań radnych z BIP Białystok"
    )
    parser.add_argument(
        "--output", default="docs/interpelacje.json",
        help="Ścieżka do pliku wyjściowego (domyślnie: docs/interpelacje.json)"
    )
    parser.add_argument(
        "--kadencja", default="IX",
        help="Kadencja: IX, VIII lub 'all' (domyślnie: IX)"
    )
    parser.add_argument(
        "--skip-details", action="store_true",
        help="Pomiń pobieranie szczegółów"
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Włącz szczegółowe logowanie"
    )
    args = parser.parse_args()

    if args.kadencja.lower() == "all":
        kadencje = list(KADENCJE.keys())
    else:
        kadencje = [k.strip() for k in args.kadencja.split(",")]

    scrape(
        kadencje=kadencje,
        output_path=args.output,
        fetch_details=not args.skip_details,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
