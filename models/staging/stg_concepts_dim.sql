{{ config(materialized='view') }}
-- Deduplicate concept_id -> (best-known) label coming from OpenAlex
select
  concept_id,
  max(concept_label_openalex) as concept_label_openalex
from {{ ref('stg_works') }}
group by concept_id
