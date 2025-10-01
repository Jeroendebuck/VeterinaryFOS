{{ config(materialized='view') }}
-- Expecting data/roster_with_metrics.csv: columns Name, OpenAlexID, Affiliation
select
  cast(r."Name" as varchar)        as name,
  cast(r."OpenAlexID" as varchar)  as openalex_id,
  cast(coalesce(r."Affiliation", '')) as affiliation
from read_csv_auto('data/roster_with_metrics.csv') as r
