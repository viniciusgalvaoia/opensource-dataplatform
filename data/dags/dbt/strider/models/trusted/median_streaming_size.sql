{{ config(
    materialized = 'table',
    database = 'iceberg'
) }}

WITH ranked_sizes AS (
  SELECT 
    size_mb / 1024 AS size_gb,
    ROW_NUMBER() OVER (ORDER BY size_mb) AS row_num,
    COUNT(*) OVER () AS total_rows
  FROM 
    {{ ref('internal_streams') }}
)
SELECT 
  AVG(size_gb) AS median_size_gb
FROM 
  ranked_sizes
WHERE 
  row_num IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2));
    