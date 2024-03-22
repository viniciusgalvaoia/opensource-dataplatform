{{ config(
    materialized = 'table',
    database = 'iceberg',
    tags = ["vendor"]
) }}

WITH non_null_nationalities AS (
SELECT
	metadata.name AS name,
	metadata.birth_date AS birth_date,
	metadata.died_at AS died_at,
	element_at(FILTER(nationalities,
	nationality -> nationality.slug IS NOT NULL),
	1) AS nationalities
FROM
	{{ source(
        'minio',
        'vendor_authors_parquet'
    ) }}
)
SELECT 
	name,
	birth_date,
	died_at,
	nationalities.id AS nationality_id,
	nationalities.label AS nationality_label,
	nationalities.slug AS nationalitiy_slug
FROM 
	non_null_nationalities
    
