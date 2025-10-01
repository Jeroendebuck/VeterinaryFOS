# etl/export_for_dashboard.py
import os
import duckdb
import pandas as pd

OUT_DIR = os.environ.get('DASHBOARD_OUT_DIR', 'site/exports')  # default for Pages deployment
os.makedirs(OUT_DIR, exist_ok=True)

con = duckdb.connect('data/warehouse.duckdb')

def export(view_name: str, filename: str) -> None:
    df = con.execute(f"select * from {view_name}").fetch_df()
    df.to_csv(os.path.join(OUT_DIR, filename), index=False)
    print(f"Exported {filename} ({len(df)} rows)")

export('dashboard_unit_concept_latest',   'unit_concept_latest.csv')
export('dashboard_gaps_portfolio',        'gaps_opportunities.csv')
export('dashboard_critical_mass_matrix',  'critical_mass_matrix.csv')

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

export('tmp_portfolio', 'portfolio_treemap.csv')
