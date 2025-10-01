{{ config(materialized='view') }}
with ranked as (
  select *,
         row_number() over (partition by unit_id, concept_id order by period desc) as rn
  from {{ ref('int_unit_concept_period') }}
),
latest as (
  select * from ranked where rn = 1
)
select
  unit_id,
  concept_id,
  case when share_global = 0 then null else share_unit / share_global end as lq,
  coalesce(active_author_hits, 0) as headcount
from latest
