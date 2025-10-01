{{ config(materialized='view') }}
select
  concept_id,
  max(concept_label_openalex) as concept_label_openalex
from {{ ref('stg_works') }}
group by concept_id
