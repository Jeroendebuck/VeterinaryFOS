{{ config(materialized='view') }}
-- Expecting data/roster_with_metrics.csv with columns incl. Name, OpenAlexID, Affiliation (optional)
select
  cast(Name as varchar) as name,
  cast(OpenAlexID as varchar) as openalex_id,
  cast(coalesce(Affiliation, '') as varchar) as affiliation
from read_csv_auto('data/roster_with_metrics.csv');
