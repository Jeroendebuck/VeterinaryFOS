{{ config(materialized='view') }}
with base as (
  select *,
    case when share_global = 0 then null else share_unit / share_global end as lq
  from {{ ref('int_unit_concept_period') }}
),
-- growth over last N periods (vars.recent_quarters)
ranked as (
  select *,
    row_number() over(partition by unit_id, concept_id order by period desc) as rn
  from base
),
latest as (
  select * from ranked where rn = 1
),
prev as (
  select * from ranked where rn = {{ var('recent_quarters') }}
)
select
  l.unit_id,
  l.concept_id,
  l.period as latest_period,
  l.lq,
  l.works as works_latest,
  /* simple CAGR proxy (works) */
  case when p.works is null or p.works = 0 then null
       else power( (1.0 * l.works) / nullif(p.works,0), 1.0/{{ var('recent_quarters') }} ) - 1 end as growth_rate
from latest l
left join prev p using (unit_id, concept_id);
