{{ config(
    materialized = 'table',
    database = 'iceberg',
    schema='trusted'
) }}


SELECT 
    COUNT(DISTINCT user_email) users_over_half_duration 
FROM 
    {{ ref('internal_streams') }}
INNER JOIN
    {{ ref('internal_movies') }}
    ON internal_streams.movie_title = internal_movies.title
WHERE
    cast(concat(substring(internal_streams.start_at, 1, 10),' ', substring(internal_streams.start_at, 12, 8)) AS TIMESTAMP) >= TIMESTAMP '2021-12-24 23:59:59' 
    AND date_diff('minute', cast(concat(substring(internal_streams.start_at, 1, 10),' ', substring(internal_streams.start_at, 12, 8)) AS TIMESTAMP), cast(concat(substring(internal_streams.end_at, 1, 10),' ', substring(internal_streams.end_at, 12, 8)) AS TIMESTAMP)) >= internal_movies.duration_mins / 2