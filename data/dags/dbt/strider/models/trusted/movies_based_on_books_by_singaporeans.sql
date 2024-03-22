{{ config(
    materialized = 'table',
    database = 'iceberg'
) }}

WITH movies_based_on_books AS (
	SELECT DISTINCT
		movie_title,
        book_title
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
	COUNT(1) movies_based_on_books_written_by_singaporeans
FROM
	streamed_movies	
INNER JOIN
	movies_based_on_books
		ON streamed_movies.movie_title = movies_based_on_books.movie_title
INNER JOIN
	{{ ref('vendor_books') }}
		ON movies_based_on_books.book_title = vendor_books.name
INNER JOIN
	{{ ref('vendor_authors') }}
		ON vendor_books.author = vendor_authors.name
        AND authors.nationality = 'nationalitiy_slug'