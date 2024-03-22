{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key='review_id',
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
    {{ dbt_utils.generate_surrogate_key([
        "books.metadata.title",
        "movies.title",
        "text"
    ]) }} AS review_id,
    text,
    rate,
    label,
    books.id as book_id,
    books.metadata.title as book_title,
    books.metadata.pages as book_pages,
    movies.id as movie_id,
    movies.title as movie_title,
    current_timestamp as reference_date 
from reviews_normalized
{% if is_incremental()%}
    WHERE current_timestamp > (SELECT MAX(reference_date) FROM {{ this }})
{% endif %}
    
