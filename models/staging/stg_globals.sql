{{ config(materialized='view') }}
-- Global totals per (concept_id, period) for LQ denominators
-- columns: period, concept_id, global_works
select * from read_csv_auto('data/globals_concepts.csv')
