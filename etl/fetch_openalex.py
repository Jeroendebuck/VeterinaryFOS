#!/usr/bin/env python3
"""
Fetch OpenAlex works for a roster of authors and materialize CSVs used by dbt.

Adds richer unit mapping so you can aggregate at the level of:
  1) Deepest institution on a work (child institute ROR if present)
  2) Roster-provided overrides per author (via seeds/author_overrides.csv)
  3) Regex alias rules on raw affiliation/institution names (via seeds/unit_aliases.csv)

Each (work × concept) row includes:
  - unit_id_auto      : unified aggregation key (ROR or custom "unit:<slug>")
  - unit_name_auto    : human label for the unit
  - raw_affiliation   : the author's raw affiliation string on that work (for QA)
  - institution_ror   : best-guess institution ROR seen on the authorship (if any)
  - concept_label_openalex : human label for the concept (OpenAlex display_name)

Environment variables:
  OPENALEX_MAILTO        (recommended) your email for OpenAlex "polite pool"
  OPENALEX_API_KEY       (optional) premium key
  OPENALEX_RATE_SECONDS  (optional) delay between page fetches (default 0.25s)
  OPENALEX_START_DATE    (optional) ISO date, default "2015-01-01"

Inputs:
  data/roster_with_metrics.csv   (must contain column: OpenAlexID)
  seeds/unit_aliases.csv         (optional; columns: pattern,unit_id,unit_name,priority)
  seeds/author_overrides.csv     (optional; columns: author_openalex_id,unit_id,unit_name)

Outputs:
  data/works.csv                 (work×concept; includes unit_id_auto etc.)
  data/globals_concepts.csv      (global concept counts by year)
"""
from __future__ import annotations

import os
import re
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from dateutil.parser import isoparse

# ---------------------------------------------------------------------
# Config / environment
# ---------------------------------------------------------------------
OPENALEX_BASE = "https://api.openalex.org"
MAILTO = os.environ.get("OPENALEX_MAILTO", "").strip()
API_KEY = os.environ.get("OPENALEX_API_KEY", "").strip()
RATE = float(os.environ.get("OPENALEX_RATE_SECONDS", "0.25"))
START_DATE = os.environ.get("OPENALEX_START_DATE", "2015-01-01")

# Paths
DATA_DIR = "data"
ROSTER_CSV = os.path.join(DATA_DIR, "roster_with_metrics.csv")
WORKS_OUT = os.path.join(DATA_DIR, "works.csv")
GLOBALS_OUT = os.path.join(DATA_DIR, "globals_concepts.csv")

ALIASES_CSV = os.path.join("seeds", "unit_aliases.csv")
OVERRIDES_CSV = os.path.join("seeds", "author_overrides.csv")

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def humanize_concept_id(cid: Optional[str]) -> Optional[str]:
    """Fallback label from an OpenAlex concept URL like https://openalex.org/C123..."""
    if not cid:
        return None
    tail = cid.split("/")[-1]
    return tail.replace("_", " ").title()


def normalize_year(pub_date: Optional[str], fallback_year: Optional[int]) -> Optional[int]:
    if pub_date:
        try:
            return isoparse(pub_date).year
        except Exception:
            pass
    return fallback_year


def _session() -> requests.Session:
    if not MAILTO:
        sys.stderr.write(
            "[warn] OPENALEX_MAILTO not set. Add your institutional email to avoid 403s.\n"
        )
    s = requests.Session()
    ua_email = f"(mailto:{MAILTO})" if MAILTO else "(mailto:someone@example.com)"
    s.headers.update({
        "User-Agent": f"VeterinaryFOS/1.0 {ua_email}",
        "Accept": "application/json",
    })
    return s

SESSION = _session()


def _get(path: str, params: Dict, max_retries: int = 6) -> requests.Response:
    """GET with retries and polite query params; raises on non-OK."""
    q = dict(params or {})
    if MAILTO:
        q.setdefault("mailto", MAILTO)
    if API_KEY:
        q.setdefault("api_key", API_KEY)

    url = f"{OPENALEX_BASE}/{path.lstrip('/')}"
    backoff = 1.0
    for _attempt in range(max_retries):
        r = SESSION.get(url, params=q, timeout=60)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        if r.status_code == 403:
            msg = r.text[:500].replace("\n", " ")
            raise requests.HTTPError(
                "403 from OpenAlex. Ensure a mailto is provided (header + query) and filter syntax is valid. "
                f"Response: {msg}",
                response=r,
            )
        r.raise_for_status()
        return r
    r.raise_for_status()


def fetch_all(endpoint: str, filter_str: str) -> List[dict]:
    """Fetch all pages for an endpoint using cursor-based pagination."""
    out: List[dict] = []
    cursor = "*"
    while True:
        params = {"per_page": 200, "cursor": cursor, "filter": filter_str}
        r = _get(endpoint, params)
        j = r.json()
        out.extend(j.get("results", []) or [])
        cursor = (j.get("meta") or {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(RATE)
    return out

# ---------------------------------------------------------------------
# Seeds: aliases & overrides (optional)
# ---------------------------------------------------------------------

def load_alias_rules(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    df = pd.read_csv(path)
    # Normalize columns and order by priority desc
    cols = {c.lower(): c for c in df.columns}
    def get(row, key):
        return row[cols[key]] if key in cols else None
    rules = []
    for _, row in df.iterrows():
        rules.append({
            "pattern": str(get(row, "pattern") or "").strip(),
            "unit_id": str(get(row, "unit_id") or "").strip(),
            "unit_name": str(get(row, "unit_name") or "").strip(),
            "priority": int(get(row, "priority") or 0),
        })
    rules.sort(key=lambda r: r.get("priority", 0), reverse=True)
    # Pre-compile regex where possible
    compiled = []
    for r in rules:
        pat = r.get("pattern") or ""
        try:
            r["_re"] = re.compile(pat) if pat else None
        except re.error:
            r["_re"] = None
        compiled.append(r)
    return compiled


def load_author_overrides(path: str) -> Dict[str, Dict[str, str]]:
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path)
    out: Dict[str, Dict[str, str]] = {}
    # Normalize column access
    cols = {c.lower(): c for c in df.columns}
    for _, row in df.iterrows():
        aid = str(row[cols.get("author_openalex_id", "author_openalex_id")]).strip()
        if not aid:
            continue
        out[aid] = {
            "unit_id": str(row[cols.get("unit_id", "unit_id")]).s
