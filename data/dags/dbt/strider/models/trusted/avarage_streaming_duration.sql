{{ config(
    materialized = 'table',
    database = 'iceberg'
) }}

SELECT 
    AVG(TIMESTAMPDIFF(HOUR, start_at, end_at)) average_streaming_duration
FROM 
	{{ ref('internal_streams') }}