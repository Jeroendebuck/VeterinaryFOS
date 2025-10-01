{{ config(materialized='view') }}

select
  m.unit_id,
  m.concept_id,
  coalesce(c.label, m.concept_id) as concept_label,
  m.latest_period,
  m.lq,
  m.works_latest,
  cm.has_critical_mass
from {{ ref('metrics_lq_growth') }} m
left join {{ ref('stg_concept_taxonomy') }} c
  on c.id = m.concept_id
left join {{ ref('metrics_critical_mass') }} cm
  using (unit_id, concept_id)
