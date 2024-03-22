{{ config(
    materialized = 'table',
    database = 'iceberg',
    schema='trusted'
) }}


SELECT 
	ROUND((approx_percentile(size_mb, 0.5) / 1000), 2) AS median_size_in_gb
FROM 
	{{ ref('internal_streams') }}
