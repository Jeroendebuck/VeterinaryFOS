{{ config(materialized='view') }}

-- Gap score: global momentum × (1 - min(1, LQ))
with g as (
  select *,
         row_number() over (partition by concept_id order by period desc) as rn
  from {{ ref('stg_globals') }}
),
latest_g as (
  select concept_id, global_works as gw_latest
  from g where rn = 1
),
prev_g as (
  select concept_id, global_works as gw_prev
  from g where rn = {{ var('recent_quarters') }}
),
momentum as (
  select
    lg.concept_id,
    case when pg.gw_prev is null or pg.gw_prev = 0 then null
         else power((1.0 * lg.gw_latest)/nullif(pg.gw_prev, 0), 1.0/{{ var('recent_quarters') }}) - 1
    end as global_growth
  from latest_g lg
  left join prev_g pg using (concept_id)
),
latest_unit as (
  select * from {{ ref('dashboard_unit_concept_latest') }}
)
select
  lu.unit_id,
  lu.concept_id,
  lu.concept_label,
  lu.lq,
  lu.works_latest,
  m.global_growth,
  (coalesce(m.global_growth, 0) * (1 - least(1, coalesce(lu.lq, 0)))) as gap_score
from latest_unit lu
left join momentum m using (concept_id)
-- DuckDB-compatible sort putting NULLs last without 'NULLS LAST'
order by (gap_score is null) asc, gap_score desc
