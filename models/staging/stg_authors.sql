{{ config(materialized='view') }}
with src as (
  select * from read_csv_auto('data/roster_with_metrics.csv', header=true, all_varchar=true)
)
select
  cast(src."Name" as varchar)       as name,
  cast(src."OpenAlexID" as varchar) as openalex_id,
  cast('' as varchar)               as affiliation
from src
