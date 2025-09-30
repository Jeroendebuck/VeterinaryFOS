{{ config(materialized='view') }}
-- headcount proxy = active_author_hits in latest period
with latest as (
  select * from {{ ref('int_unit_concept_period') }}
  qualify row_number() over(partition by unit_id, concept_id order by period desc) = 1
)
select
  unit_id,
  concept_id,
  (case when share_global = 0 then null else share_unit / share_global end) as lq,
  coalesce(active_author_hits,0) as headcount
from latest;
