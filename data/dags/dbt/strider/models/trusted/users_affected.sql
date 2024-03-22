{{ config(
    materialized = 'table',
    database = 'iceberg',
	schema='trusted'
) }}

SELECT
	count(DISTINCT user_email) total_users_affected
FROM
	{{ ref('internal_streams') }}
WHERE
	movie_title = 'Unforgiven'
	AND (
		cast(concat(substring(start_at, 1, 10),
	' ',
	substring(start_at, 12, 8)) AS TIMESTAMP) BETWEEN TIMESTAMP '2021-12-25 07:00:00' and TIMESTAMP '2021-12-25 12:00:00'
		or cast(concat(substring(end_at, 1, 10),
		' ',
		substring(end_at, 12, 8)) AS TIMESTAMP) BETWEEN TIMESTAMP '2021-12-25 07:00:00' and TIMESTAMP '2021-12-25 12:00:00'
    )