{{ config(materialized='view') }}

with base as (
  select *,
         coalesce(active_author_hits, 0) as active_hits
  from {{ ref('int_unit_concept_period') }}
),
ranked as (
  select *,
         row_number() over (partition by unit_id, concept_id order by period desc) as rn
  from base
)
select
  unit_id,
  concept_id,
  period as latest_period,
  active_hits,
  (active_hits >= {{ var('critical_mass_threshold') }}) as has_critical_mass
from ranked
where rn = 1
