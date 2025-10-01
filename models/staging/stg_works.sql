{{ config(materialized='view') }}
-- ETL creates data/works.csv with columns:
-- work_id, published_date (YYYY-MM-DD), year, author_openalex_id, institution_ror, concept_id, concept_level
-- Each row = (work × concept)
select * from read_csv_auto('data/works.csv')
