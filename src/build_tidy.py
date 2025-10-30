#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AP2Y tidy builder.
- Read all CSVs from ./data/raw
- Vintage date taken directly from filename (e.g. AP2Y_YYYY-MM-DD.csv).
  The fetch step already extracted each file’s "Release date" from the CSV header
  and embedded it into the filename, so this script relies solely on filenames for identifying vintages.

- Safeguards (not required for this dataset but included for future robustness):
  Table begins at the first line like "2021 MAY" or "2021 AUGUST", correctly handling both short and long month names.
  Forward-fill applied as a safeguard against missing values.
  Strip any time component (HH:MM:SS) from vintage_date.

- Output = ./data/processed/AP2Y_tidy.csv
- This script overwrites the output unlike the fetch step; since processed data are derived artifacts, this is more practical.
"""

import re
from pathlib import Path
from io import StringIO
import pandas as pd

RAW_DIR = Path("./data/raw")        # raw data path (when executed from the repository root)
OUT_PATH = Path("./data/processed/AP2Y_tidy.csv")  # output path (when executed from the repository root)
# Safeguard: ensure both raw and processed directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)


# ---------- Vintage date parsers ---------- 
def parse_vintage_from_filename(name: str) -> pd.Timestamp:
    """Parse the vintage (release) date from a filename ending in 'YYYY-MM-DD.csv'."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})\.csv$", name)
    if not m:
        return pd.NaT
    ts = pd.to_datetime(m.group(1), format="%Y-%m-%d", errors="coerce")
    return pd.Timestamp(ts.date()) if pd.notna(ts) else pd.NaT


def parse_month_label(s) -> pd.Timestamp:
    """Parse 'YYYY MON' or 'YYYY MONTH' to first day of month."""
    if not isinstance(s, str):
        return pd.NaT
    t = re.sub(r"\s+", " ", s.strip()).upper()
    # Try short month name (e.g., '2024 AUG', as observed in the raw files)
    ts = pd.to_datetime(t, format="%Y %b", errors="coerce")
    if pd.notna(ts):
        return pd.Timestamp(ts.date())
    # Safeguard: try full month name (e.g., '2024 AUGUST') 
    ts = pd.to_datetime(t, format="%Y %B", errors="coerce")
    return pd.Timestamp(ts.date()) if pd.notna(ts) else pd.NaT


# ---------- Core routine ---------- 
def main():
    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        print("[warn] no CSVs under ./data/raw")
        return

    parts = []

    for fp in files:
        text = fp.read_text(encoding="utf-8", errors="ignore")
        # Parse vintage date from filenames
        vintage = parse_vintage_from_filename(fp.name)
        # If fail, skip with notice
        if pd.isna(vintage):
            print(f"[skip] {fp.name}: vintage missing")
            continue

        # Find first line like 'YYYY MON' or 'YYYY MONTH'
        lines = text.splitlines()
        start = None
        pat = re.compile(r"^\s*\d{4}\s+[A-Za-z]{3,9}\b")
        for i, ln in enumerate(lines):
            if pat.match(ln):
                start = i
                break
        if start is None:
            # Fallback: after first blank line
            for i, ln in enumerate(lines):
                if not ln.strip():
                    start = i + 1
                    break
        if start is None:
            start = 0

        body = "\n".join(lines[start:])
        df = pd.read_csv(StringIO(body), engine="python")
        if df.empty or df.shape[1] < 2:
            print(f"[skip] {fp.name}: unexpected shape")
            continue

        # Column choices: first column = month label, first numeric column = value
        df.columns = [str(c).strip() for c in df.columns]
        date_col = df.columns[0]
        value_col = None
        for c in df.columns[1:]:
            s = pd.to_numeric(pd.Series(df[c]).astype(str).str.replace(",", ""), errors="coerce")
            if s.notna().sum() > 0:
                df[c] = s
                value_col = c
                break
        if value_col is None:
            print(f"[skip] {fp.name}: no numeric-like value column")
            continue

        # Month parse
        dt = pd.Series(df[date_col]).map(parse_month_label)
        keep = dt.notna()
        if not keep.any():
            print(f"[skip] {fp.name}: no parsable month labels")
            continue

        sub = pd.DataFrame({
            "observation_month": dt[keep].dt.to_period("M").astype(str),
            "value": df.loc[keep, value_col],
        }).dropna(subset=["value"])

        # Duplicates: keep last per month
        sub = (sub.sort_values("observation_month")
                   .drop_duplicates(subset=["observation_month"], keep="last"))
        sub["vintage_date"] = vintage
        parts.append(sub)

        print(f"[ok] {fp.name}: rows={len(sub)} vintage={vintage.date()}")

    if not parts:
        print("[error] nothing parsed")
        return

    # Concatenate and deduplication
    tidy = pd.concat(parts, ignore_index=True)[["observation_month", "vintage_date", "value"]]

    tidy["observation_month"] = pd.PeriodIndex(tidy["observation_month"], freq="M")

    # Safeguard: strip any time component (HH:MM:SS) from vintage_date,
    # just in case ONS changes the release date header format.
    tidy["vintage_date"] = pd.to_datetime(tidy["vintage_date"]).dt.normalize()
   
    tidy = (tidy.sort_values(["observation_month", "vintage_date"])
               .drop_duplicates(subset=["observation_month", "vintage_date"], keep="last"))

    # Densify month×vintage grid, then forward-fill across vintages
    all_months = pd.period_range(tidy["observation_month"].min(),
                                 tidy["observation_month"].max(), freq="M")
    all_vintages = pd.Index(tidy["vintage_date"].unique()).sort_values()
    idx = pd.MultiIndex.from_product([all_months, all_vintages],
                                     names=["observation_month", "vintage_date"])
    
    tidy = (
        tidy.set_index(["observation_month", "vintage_date"])
            .reindex(idx)
            .sort_index()
    )   

    # Safeguard: forward-fill missing values within each observation_month
    tidy["value"] = tidy.groupby("observation_month", group_keys=False)["value"].ffill()
    # Drop missing value rows where ffill not possible
    tidy = tidy.dropna(subset=["value"]).reset_index()

    # Data type enforcement for value and observation_month
    tidy["value"] = pd.to_numeric(tidy["value"], errors="raise")
    tidy["observation_month"] = pd.PeriodIndex(tidy["observation_month"], freq='M').to_timestamp()

    # Write a single output file — overwriting the previous one (unlike the fetch step),
    # since processed data are derived artifacts and overwriting is more practical.
    tidy.to_csv(OUT_PATH, index=False)
    print(f"[done] {len(tidy)} rows -> {OUT_PATH}")

if __name__ == "__main__":
    main()

