#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AP2Y top-N fetcher.
- Edit NUM_VINTAGES / OUTDIR below.
- Scrapes top-N (NUM_VINTAGES) CSV links (Latest + Previous) in page order.
- File name is taken from 'AP2Y_Release date' in the CSV header (AP2Y_YYYY-MM-DD.csv); falls back to 'Date superseded'.
- Existing files are skipped to preserve previously downloaded vintages (no overwrites).
- If neither is available, the file is skipped (no today-date fallback to avoid overwrites).
"""

import re
import time
from pathlib import Path
from typing import Optional, List, Dict

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

# --------------------- Parameters --------------------- 
NUM_VINTAGES = 30                 # process the top N links including "Latest version"
OUTDIR = Path("./data/raw")       # output directory (when executed from the repository root) 
THROTTLE = 2.5                    # delay between requests to lower the chance of HTTP 429 (1.2 -> 2.5)
RETRY_ATTEMPTS = 3                # per-file attempts for transient errors
RETRY_PAUSE = 5.0                 # seconds between retries (10.0 -> 5.0)
SERIES_CODE = "AP2Y"              # Data series code
# ----------------------------------------------------- 

ONS_PREVIOUS_URL = (
    "https://www.ons.gov.uk/employmentandlabourmarket/"
    "peopleinwork/employmentandemployeetypes/timeseries/ap2y/lms/previous"
)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; boe-3h-min/1.1)"}


def extract_release_date_from_header(csv_text: str) -> Optional[str]:
    """Return ISO date from 'Release date' in the CSV metadata header; None if absent/unparsable."""
    for line in csv_text.splitlines():
        if not line.strip():      # stop at the first blank line (table begins)
            break
        m = re.search(r"Release date,?\s*(.+)", line, flags=re.I)
        if m:
            try:
                return dateparser.parse(m.group(1), dayfirst=True, fuzzy=True).date().isoformat()
            except Exception:
                return None
    return None


def get_all_csv_links_with_superseded_date() -> List[Dict]:
    """
    Scrape the CSV links (covers Latest + Previous).
    Also capture 'Date superseded' as a fallback.
    """
    r = requests.get(ONS_PREVIOUS_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    links: List[Dict] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "generator" in href and "format=csv" in href:
            url = requests.compat.urljoin(ONS_PREVIOUS_URL, href)
            vicinity = a.parent.get_text(" ", strip=True) if a.parent else ""
            m = re.search(r"(\d{1,2}\s+\w+\s+\d{4})", vicinity)
            cand = None
            if m:
                try:
                    cand = dateparser.parse(m.group(1), dayfirst=True, fuzzy=True).date().isoformat()
                except Exception:
                    cand = None
            links.append({"url": url, "superseded_date": cand})

    # De-duplicate while preserving order
    seen, out = set(), []
    for e in links:
        if e["url"] not in seen:
            seen.add(e["url"])
            out.append(e)
    return out


def main():
    outdir = (Path.cwd() / OUTDIR).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    print(f"[info] Output directory: {outdir}")

    try:
        entries = get_all_csv_links_with_superseded_date()
    except Exception as e:
        print(f"[error] failed to scrape links: {e}")
        return

    if not entries:
        print("[warn] no CSV links found.")
        return

    # process the top-N links
    links_to_fetch = entries[:NUM_VINTAGES]
    total = len(links_to_fetch)

    saved = 0
    skipped_exists = 0
    skipped_nodate = 0
    failed = 0

    for idx, ent in enumerate(links_to_fetch, start=1):
        url = ent["url"]
        print(f"[{idx}/{total}] GET {url}")

        text = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                r = requests.get(url, headers=HEADERS, timeout=60)
                # Retry in case of HTTP 429 / 503
                if r.status_code in (429, 503):
                    print(f"[warn] HTTP {r.status_code}, retry {attempt}/{RETRY_ATTEMPTS}")
                    time.sleep(RETRY_PAUSE)
                    continue
                r.raise_for_status()
                text = r.text
                break
            except Exception as e:
                print(f"[warn] attempt {attempt} failed: {e}")
                time.sleep(RETRY_PAUSE)
        # when retry attempts exhausted -> skip files gracefully
        if text is None:
            print(f"[skip] giving up: {url}")
            failed += 1
            time.sleep(THROTTLE)
            continue

        # name files using 'Release date' -> 'Date superseded' -> else skip
        release_iso = extract_release_date_from_header(text)
        vintage_iso = release_iso or ent.get("superseded_date")
        if not vintage_iso:
            print(f"[skip] no 'Release date' in header and no 'Date superseded' nearby.")
            skipped_nodate += 1
            time.sleep(THROTTLE)
            continue
        
        # save file as AP2Y_YYYY-MM-DD.csv
        fname = f"{SERIES_CODE}_{vintage_iso}.csv"
        target = outdir / fname

        # skip existing files
        if target.exists():
            print(f"[skip] {fname} already exists.")
            skipped_exists += 1
        else:
            target.write_text(text, encoding="utf-8")
            print(f"[save] {fname}")
            saved += 1

        time.sleep(THROTTLE)

    print(f"[done] Processed top {total} link(s): saved={saved}, exists={skipped_exists}, no-date={skipped_nodate}, failed={failed}")


if __name__ == "__main__":
    main()

