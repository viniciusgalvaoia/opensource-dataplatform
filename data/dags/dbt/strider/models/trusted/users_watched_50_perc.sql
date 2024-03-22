{{ config(
    materialized = 'table',
    database = 'iceberg'
) }}


SELECT 
	count(DISTINCT user_email)
FROM 
	{{ ref('internal_streams') }}
INNER JOIN
	{{ ref('internal_movies') }}
		ON internal_streams.movie_title = internal_movies.title
        AND internal_streams.start_at >= DATE_SUB('2021-12-31 23:59:59', INTERVAL 7 DAY)
        AND TIMESTAMPDIFF(MINUTE, internal_streams.start_at, internal_streams.end_at) >= internal_movies.duration_mins/2