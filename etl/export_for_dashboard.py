# etl/export_for_dashboard.py
import os
import duckdb
import pandas as pd

os.makedirs('exports', exist_ok=True)
con = duckdb.connect('data/warehouse.duckdb')

def export(view_name: str, out_path: str) -> None:
    df = con.execute(f"select * from {view_name}").fetch_df()
    df.to_csv(out_path, index=False)
    print(f"Exported {out_path} ({len(df)} rows)")

# Export dbt views
export('dashboard_unit_concept_latest',   'exports/unit_concept_latest.csv')
export('dashboard_gaps_portfolio',        'exports/gaps_opportunities.csv')
export('dashboard_critical_mass_matrix',  'exports/critical_mass_matrix.csv')

# Build a treemap-friendly table on the fly
con.execute("""
create or replace view tmp_portfolio as
select
  c.label as node,
  c.level as level,
  m.unit_id,
  sum(m.works_latest) as size
from dashboard_unit_concept_latest m
join stg_concept_taxonomy c
  on c.id = m.concept_id
group by 1,2,3
""")

export('tmp_portfolio', 'exports/portfolio_treemap.csv')
