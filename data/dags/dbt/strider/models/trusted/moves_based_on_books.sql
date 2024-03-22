{{ config(
    materialized = 'table',
    database = 'iceberg'
) }}

WITH movies_based_on_books AS (
	SELECT DISTINCT
		movie_title
    FROM
		{{ ref('vendor_reviews') }}
),
streamed_movies AS (
	SELECT DISTINCT
		movie_title
    FROM
		{{ ref('internal_streams') }}
)
SELECT
	COUNT(*) total_streamed_movies,
    COUNT(movies_based_on_books.movie_title) total_streamed_movies_based_on_books,
    (COUNT(movies_based_on_books.movie_title) / COUNT(1)) * 100 percentage_of_streamed_movies_based_on_books
FROM
	streamed_movies
LEFT JOIN
	movies_based_on_books
		ON streamed_movies.movie_title = movies_based_on_books.movie_title