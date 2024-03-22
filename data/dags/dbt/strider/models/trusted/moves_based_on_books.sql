{{ config(
    materialized = 'table',
    database = 'iceberg',
	schema='trusted'
) }}

WITH movies_based_on_books AS (
	SELECT DISTINCT
		LOWER(movie_title) as movie_title
    FROM
		{{ ref('vendor_reviews') }}
),
streamed_movies AS (
	SELECT DISTINCT
		LOWER(movie_title) as movie_title
    FROM
		{{ ref('internal_streams') }}
)
SELECT
	COUNT(*) total_streamed_movies,
    COUNT(movies_based_on_books.movie_title) total_streamed_movies_based_on_books,
    (CAST(COUNT(movies_based_on_books.movie_title) AS DOUBLE) / COUNT(*))  percentage_of_streamed_movies_based_on_books
FROM
	streamed_movies
LEFT JOIN
	movies_based_on_books
		ON streamed_movies.movie_title = movies_based_on_books.movie_title