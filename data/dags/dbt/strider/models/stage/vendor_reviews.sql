{{ config(
    materialized = 'table',
    database = 'iceberg',
    tags = ["vendor"]
) }}

with reviews_normalized as (
select 
	content.text as text, 
	rating.rate as rate, 
	rating.label as label, 
	element_at(books, 1) as books,
	element_at(filter(movies, movie -> movie.title <> 'end'), 1) as movies
from 
    {{ source(
        'minio',
        'vendor_reviews_parquet'
    ) }}
)

select 
    text,
    rate,
    label,
    books.id as book_id,
    books.metadata.title as book_title,
    books.metadata.pages as book_pages,
    movies.id as movie_id,
    movies.title as movie_title
from reviews_normalized
    
