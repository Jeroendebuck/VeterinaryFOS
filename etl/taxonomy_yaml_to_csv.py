import yaml, pandas as pd
from pathlib import Path

src = Path("data/vet_taxonomy.yaml")
dst = Path("seeds/concept_taxonomy.csv")
obj = yaml.safe_load(src.read_text())
rows = []
for t in obj:
    rows.append({
        "id": t.get("id"),
        "label": t.get("label"),
        "level": t.get("level"),
        "parents": ",".join(t.get("parents") or []),
        "synonyms": "|".join(t.get("synonyms") or []),
        "include_keywords": "|".join(t.get("include_keywords") or []),
        "exclude_keywords": "|".join(t.get("exclude_keywords") or []),
    })

pd.DataFrame(rows).to_csv(dst, index=False)
print(f"Wrote {dst}")
