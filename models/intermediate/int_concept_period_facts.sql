{{ config(materialized='view') }}
with unit_works as (
  select
    w.institution_ror as unit_id,
    w.concept_id,
    w.year as period,
    count(distinct w.work_id) as works,
    count(distinct case
      when try_cast(w.published_date as date)
           >= date_trunc('year', current_date) - INTERVAL 1 YEAR
      then w.work_id end) as works_lastyear,
    count(distinct w.author_openalex_id) as authors_touching
  from {{ ref('stg_works') }} w
  where w.concept_level in (2, 3)
  group by 1,2,3
)
select * from unit_works
