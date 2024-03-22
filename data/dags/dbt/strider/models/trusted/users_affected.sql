{{ config(
    materialized = 'table',
    database = 'iceberg'
) }}

SELECT
	count(DISTINCT user_email) total_users_affected
FROM
	{{ ref('internal_streams') }}
WHERE
	movie_title = 'Unforgiven'
    AND (
		start_at BETWEEN '2021-12-25 07:00:00' AND '2021-12-25 12:00:00'
		OR end_at BETWEEN '2021-12-25 07:00:00' AND '2021-12-25 12:00:00'
    )