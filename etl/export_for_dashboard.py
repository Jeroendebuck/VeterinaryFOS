import duckdb, os
import pandas as pd

os.makedirs('exports', exist_ok=True)
con = duckdb.connect('data/warehouse.duckdb')

# Helper to export a view
def export(name, out):
    df = con.execute(f"select * from {name}").fetch_df()
    df.to_csv(out, index=False)
    print(f"Exported {out} ({len(df)} rows)")

export('dashboard_unit_concept_latest', 'exports/unit_concept_latest.csv')
export('dashboard_gaps_portfolio', 'exports/gaps_opportunities.csv')
export('dashboard_critical_mass_matrix', 'exports/critical_mass_matrix.csv')

# Portfolio treemap = sum works by label level (uses concept label)
con.execute("""create or replace view tmp_portfolio as
select c.label as node, c.level as level, u.unit_id, sum(m.works_latest) as size
from dashboard_unit_concept_latest m
join stg_concept_taxonomy c on c.id = m.concept_id
group by 1,2,3;
"""\)
export('tmp_portfolio', 'exports/portfolio_treemap.csv')
