#!/usr/bin/env python3
"""
Fetch OpenAlex works for a roster of authors and materialize CSVs used by dbt.

Key behaviors
- Uses a compliant User-Agent with a mailto (required by OpenAlex polite pool)
- Adds `mailto` (and optional `api_key`) as query params on every request
- Retries transient errors with exponential backoff
- Correctly passes `from_publication_date` inside the `filter` parameter
- Writes:
    data/works.csv             (work×concept rows)
    data/globals_concepts.csv  (global concept counts by year)

Environment variables
- OPENALEX_MAILTO         (required) your email, e.g., "you@school.edu"
- OPENALEX_API_KEY        (optional) premium key
- OPENALEX_RATE_SECONDS   (optional) delay between paged requests (default 0.25)
- OPENALEX_START_DATE     (optional) ISO date for filter window start (default 2015-01-01)

Inputs
- data/roster_with_metrics.csv with a column `OpenAlexID` (e.g., https://openalex.org/A1234567890)

Outputs
- data/works.csv with columns:
    work_id, published_date, year, author_openalex_id, institution_ror, concept_id, concept_level
- data/globals_concepts.csv with columns:
    concept_id, period, global_works
"""
from __future__ import annotations

import os
import sys
import time
from typing import Dict, Iterable, List

import pandas as pd
import requests
from dateutil.parser import isoparse

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

def humanize_concept_id(cid: str | None) -> str | None:
    if not cid:
        return None
    tail = cid.split("/")[-1]
    return tail.replace("_", " ").title()

def _session() -> requests.Session:
    if not MAILTO:
        sys.stderr.write(
            "[warn] OPENALEX_MAILTO not set. Add your institutional email to avoid 403s.\n"
        )
    s = requests.Session()
    ua_email = f"(mailto:{MAILTO})" if MAILTO else "(mailto:[email protected])"
    s.headers.update({
        "User-Agent": f"VetResearchLandscape/1.0 {ua_email}",
        "Accept": "application/json",
    })
    return s


SESSION = _session()


def _get(path: str, params: Dict, max_retries: int = 6) -> requests.Response:
    """GET with retries and polite query params.

    Always includes `mailto` and `api_key` as query params in addition to the
    User-Agent header, because some OpenAlex edges rely on the explicit param.
    """
    q = dict(params or {})
    if MAILTO:
        q.setdefault("mailto", MAILTO)
    if API_KEY:
        q.setdefault("api_key", API_KEY)

    url = f"{OPENALEX_BASE}/{path.lstrip('/')}"
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        r = SESSION.get(url, params=q, timeout=60)
        # Retry on transient errors
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        # Helpful message for 403s
        if r.status_code == 403:
            msg = r.text[:500].replace("\n", " ")
            raise requests.HTTPError(
                "403 from OpenAlex. Ensure a mailto is provided via User-Agent and as a query param; "
                f"also check filter syntax. Response: {msg}",
                response=r,
            )
        r.raise_for_status()
        return r
    r.raise_for_status()


def fetch_all(endpoint: str, filter_str: str) -> List[dict]:
    """Fetch all pages for a given endpoint using cursor pagination."""
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


def read_roster_ids(path: str) -> List[str]:
    df = pd.read_csv(path)
    if "OpenAlexID" not in df.columns:
        raise SystemExit(f"Missing 'OpenAlexID' column in {path}")
    ids = [str(x).strip() for x in df["OpenAlexID"].dropna().tolist()]
    # Filter obviously bad values
    ids = [i for i in ids if i and i != "nan"]
    if not ids:
        raise SystemExit("No OpenAlex IDs found in roster.")
    return ids


def normalize_year(pub_date: str | None, fallback_year: int | None) -> int | None:
    if pub_date:
        try:
            return isoparse(pub_date).year
        except Exception:
            pass
    return fallback_year


def harvest_for_author(author_id: str) -> Iterable[dict]:
    """Yield work×concept rows for one author since START_DATE."""
    # NOTE: from_publication_date must be inside `filter`, not top-level.
    filter_str = f"authorships.author.id:{author_id},from_publication_date:{START_DATE}"
    works = fetch_all("works", filter_str)

    for w in works:
        wid = w.get("id")
        pub_date = (
            w.get("publication_date")
            or w.get("from_publication_date")
            or (w.get("host_venue") or {}).get("published_date")
        )
        year = normalize_year(pub_date, w.get("publication_year"))

        # Find the institution for this author on this work (first listed)
        inst_ror = None
        for au in (w.get("authorships") or []):
            if (au.get("author") or {}).get("id") == author_id:
                insts = au.get("institutions") or []
                if insts:
                    inst_ror = insts[0].get("ror") or insts[0].get("id")
                break

        # Expand concepts (each concept becomes its own row)
        for c in (w.get("concepts") or []):
            cid = c.get("id")
            clevel = c.get("level")
            if cid is None or clevel is None:
                continue
            yield {
                "work_id": wid,
                "published_date": pub_date,
                "year": year,
                "author_openalex_id": author_id,
                "institution_ror": inst_ror,
                "concept_id": cid,
                "concept_level": clevel,
                "concept_label_openalex": cname,  
            }


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    author_ids = read_roster_ids(ROSTER_CSV)

    rows: List[dict] = []
    for idx, aid in enumerate(author_ids, 1):
        print(f"[{idx}/{len(author_ids)}] Fetching works for {aid} since {START_DATE}…", flush=True)
        try:
            rows.extend(list(harvest_for_author(aid)))
        except requests.HTTPError as e:
            # Surface which author caused the failure; continue after a brief pause
            sys.stderr.write(f"ERROR for {aid}: {e}\n")
            time.sleep(2)
            continue

    # Write works.csv
    df = pd.DataFrame(rows)
    df.to_csv(WORKS_OUT, index=False)
    print(f"Wrote {WORKS_OUT} ({len(df)} rows)")

    # Write globals_concepts.csv (global denominators by concept×year)
    if not df.empty:
        g = (
            df.groupby(["concept_id", "year"], dropna=False)
              .agg(global_works=("work_id", "nunique"))
              .reset_index()
              .rename(columns={"year": "period"})
        )
    else:
        g = pd.DataFrame(columns=["concept_id", "period", "global_works"])

    g.to_csv(GLOBALS_OUT, index=False)
    print(f"Wrote {GLOBALS_OUT} ({len(g)} rows)")


if __name__ == "__main__":
    main()
