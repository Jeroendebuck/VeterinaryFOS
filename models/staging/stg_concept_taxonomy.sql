{{ config(materialized='view') }}
-- Keep taxonomy fields as text for simplicity
select
  id,
  label,
  level,
  parents,
  synonyms,
  include_keywords,
  exclude_keywords
from {{ ref('concept_taxonomy') }}

