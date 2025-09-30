{{ config(materialized='view') }}
select
  id,
  label,
  level,
  split(parents, ',') as parents,
  split(coalesce(synonyms,''), '|') as synonyms,
  split(coalesce(include_keywords,''), '|') as include_keywords,
  split(coalesce(exclude_keywords,''), '|') as exclude_keywords
from {{ ref('concept_taxonomy') }};
