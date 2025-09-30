{{ config(materialized='view') }}
-- Compute shares for LQ, and active authors in a rolling 4-quarter window
with facts as (
  select * from {{ ref('int_concept_period_facts') }}
),
unit_totals as (
  select unit_id, period, sum(works) as works_all
  from facts
  group by 1,2
),
globals as (
  select period, concept_id, sum(global_works) as global_works
  from {{ ref('stg_globals') }}
  group by 1,2
)
select
  f.unit_id,
  f.concept_id,
  f.period,
  f.works,
  ut.works_all,
  g.global_works,
  /* shares for LQ */
  (1.0 * f.works) / nullif(ut.works_all,0) as share_unit,
  (1.0 * g.global_works) / nullif(sum(g.global_works) over(partition by g.period),0) as share_global,
  /* rolling activity: approximate via last 4 periods availability */
  sum(f.authors_touching) over(partition by f.unit_id, f.concept_id order by f.period rows between 3 preceding and current row) as active_author_hits
from facts f
join unit_totals ut using (unit_id, period)
left join globals g using (period, concept_id);
