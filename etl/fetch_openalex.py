import os, time, requests, sys
from urllib.parse import urlencode
from dateutil.parser import isoparse
import pandas as pd

OPENALEX = "https://api.openalex.org"
RATE = float(os.environ.get("OPENALEX_RATE_SECONDS", "0.25"))  # seconds between calls
MAILTO = os.environ.get("OPENALEX_MAILTO", "").strip()
API_KEY = os.environ.get("OPENALEX_API_KEY", "").strip()

if not MAILTO:
    sys.stderr.write(
        "[warn] OPENALEX_MAILTO not set. Add your email to use the 'polite pool' and avoid 403s.\n"
    )

# One session with a compliant User-Agent
session = requests.Session()
ua_email = f"(mailto:{MAILTO})" if MAILTO else "(mailto:[email protected])"
session.headers.update({
    "User-Agent": f"VeterinaryFOS/1.0 {ua_email}",
    "Accept": "application/json"
})

def _get(url, params, max_retries=5):
    # Always include mailto/api_key as query params too (OpenAlex accepts these)
    q = dict(params or {})
    if MAILTO:
        q.setdefault("mailto", MAILTO)
    if API_KEY:
        q.setdefault("api_key", API_KEY)

    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        r = session.get(url, params=q, timeout=60)
        # Retry on common transient codes or Cloudflare blocks
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        if r.status_code == 403:
            # Often caused by missing/invalid User-Agent or mailto; surface helpful message
            msg = r.text[:500].replace("\n", " ")
            raise requests.HTTPError(
                f"403 from OpenAlex. Ensure you're sending a User-Agent with mailto and a mailto param. Response: {msg}",
                response=r
            )
        r.raise_for_status()
        return r
    r.raise_for_status()

def fetch_all(endpoint, params):
    cursor = "*"
    results = []
    while True:
        p = dict(params)
        p.update({"per_page": 200, "cursor": cursor})
        url = f"{OPENALEX}/{endpoint}"
        r = _get(url, p)
        j = r.json()
        results.extend(j.get("results", []))
        cursor = j.get("meta", {}).get("next_cursor")
        if not cursor:
            break
        time.sleep(RATE)
    return results

roster_path = "data/roster_with_metrics.csv"
works_out = "data/works.csv"
globals_out = "data/globals_concepts.csv"

# Load roster
roster = pd.read_csv(roster_path)
auth_ids = [str(x).strip() for x in roster.get("OpenAlexID", []) if pd.notna(x)]

rows = []
for aid in auth_ids:
    # works authored by this person (last 10y for MVP; adjust)
    res = fetch_all("works", {
        "filter": f"authorships.author.id:{aid}",
        "from_publication_date": "2015-01-01",
    })
    for w in res:
        wid = w.get("id")
        pub_date = w.get("publication_date") or w.get("from_publication_date") or (w.get("host_venue", {}) or {}).get("published_date")
        year = None
        try:
            year = isoparse(pub_date).year if pub_date else w.get("publication_year")
        except Exception:
            year = w.get("publication_year")
        # best institution per authorship (first inst of matching author)
        inst_ror = None
        for au in (w.get("authorships") or []):
            if (au.get("author") or {}).get("id") == aid:
                insts = au.get("institutions") or []
                if insts:
                    inst_ror = insts[0].get("ror") or insts[0].get("id")
                break
        # concepts
        concepts = w.get("concepts") or []
        for c in concepts:
            cid = c.get("id")
            clevel = c.get("level")
            if cid and clevel is not None:
                rows.append({
                    "work_id": wid,
                    "published_date": pub_date,
                    "year": year,
                    "author_openalex_id": aid,
                    "institution_ror": inst_ror,
                    "concept_id": cid,
                    "concept_level": clevel,
                })

# Save works
os.makedirs("data", exist_ok=True)
pd.DataFrame(rows).to_csv(works_out, index=False)

# Build a simple global concepts table (same periods present in works)
df = pd.DataFrame(rows)
if not df.empty:
    g = df.groupby(["concept_id", "year"], dropna=False).agg(global_works=("work_id", "nunique")).reset_index()
    g.rename(columns={"year": "period"}, inplace=True)
    g.to_csv(globals_out, index=False)
else:
    pd.DataFrame(columns=["concept_id", "period", "global_works"]).to_csv(globals_out, index=False)

print(f"Wrote {works_out} and {globals_out}")
