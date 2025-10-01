import os
import duckdb
import pandas as pd

OUT_DIR = os.environ.get('DASHBOARD_OUT_DIR', 'site/exports')
os.makedirs(OUT_DIR, exist_ok=True)
con = duckdb.connect('data/warehouse.duckdb')

def export(view_name: str, filename: str) -> None:
    df = con.execute(f"select * from {view_name}").fetch_df()
    df.to_csv(os.path.join(OUT_DIR, filename), index=False)
    print(f"Exported {filename} ({len(df)} rows)")

# Export dbt views
export('dashboard_unit_concept_latest',   'unit_concept_latest.csv')
export('dashboard_gaps_portfolio',        'gaps_opportunities.csv')
export('dashboard_critical_mass_matrix',  'critical_mass_matrix.csv')

# Build a simple treemap source without taxonomy join
con.execute("""
create or replace view tmp_portfolio as
select
  concept_label as node,
  1 as level,
  unit_id,
  sum(works_latest) as size
from dashboard_unit_concept_latest
group by 1,2,3
""")
export('tmp_portfolio', 'portfolio_treemap.csv')
