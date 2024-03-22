{{ config(
    materialized = 'table',
    database = 'iceberg',
	schema='trusted'
) }}

WITH movies_based_on_books AS (
	SELECT DISTINCT
		LOWER(movie_title) as movie_title,
        LOWER(book_title) as book_title
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
	COUNT(1) movies_based_on_books_written_by_singaporeans
FROM
	streamed_movies	
INNER JOIN
	movies_based_on_books
		ON streamed_movies.movie_title = movies_based_on_books.movie_title
INNER JOIN
	{{ ref('vendor_books') }}
		ON movies_based_on_books.book_title = LOWER(vendor_books.name)
INNER JOIN
	{{ ref('vendor_authors') }}
		ON LOWER(vendor_books.author) = LOWER(vendor_authors.name)
        AND vendor_authors.nationalitiy_slug = 'singaporeans'