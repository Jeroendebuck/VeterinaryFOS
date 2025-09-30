{{ config(materialized='view') }}
-- Aggregate author works into unit (institution) × concept × period facts
with unit_works as (
  select
    w.institution_ror as unit_id,
    w.concept_id,
    w.year as period,
    count(distinct w.work_id) as works,
    count(distinct case when w.published_date >= date_trunc('year', now()) - interval '1 year' then w.work_id end) as works_lastyear,
    count(distinct w.author_openalex_id) as authors_touching
  from {{ ref('stg_works') }} w
  where w.concept_level in (2,3) -- focus on meso/fine levels; adjust as needed
  group by 1,2,3
)
select * from unit_works;
