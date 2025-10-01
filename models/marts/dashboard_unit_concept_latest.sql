{{ config(materialized='view') }}

with base as (
  select
    m.unit_id,
    m.concept_id,
    coalesce(c.label, d.concept_label_openalex, m.concept_id) as concept_label,
    m.latest_period,
    m.lq,
    m.works_latest
  from {{ ref('metrics_lq_growth') }} m
  left join {{ ref('stg_concept_taxonomy') }} c
    on c.id = m.concept_id
  left join {{ ref('stg_concepts_dim') }} d
    on d.concept_id = m.concept_id
)
select
  b.unit_id,
  b.concept_id,
  b.concept_label,
  b.latest_period,
  b.lq,
  b.works_latest,
  cm.has_critical_mass
from base b
left join {{ ref('metrics_critical_mass') }} cm
  on cm.unit_id = b.unit_id
 and cm.concept_id = b.concept_id
