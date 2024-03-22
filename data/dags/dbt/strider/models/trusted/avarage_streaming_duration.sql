{{ config(
    materialized = 'table',
    database = 'iceberg',
    schema='trusted'
) }}

SELECT 
    AVG(CAST(date_diff('hour', cast(concat(substring(start_at, 1, 10),' ', substring(start_at, 12, 8)) AS TIMESTAMP), 
    cast(concat(substring(end_at, 1, 10),' ', substring(end_at, 12, 8)) AS TIMESTAMP)) AS double)) AS average_streaming_duration
FROM 
	{{ ref('internal_streams') }}