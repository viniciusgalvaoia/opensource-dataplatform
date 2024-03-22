{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key='book_id',
    database = 'iceberg',
    tags = ["vendor"]
) }}


SELECT
    {{ dbt_utils.generate_surrogate_key([
        "name",
        "author"
    ]) }} AS book_id,
	name,
    pages,
    author,
    publisher,
    current_timestamp as reference_date 
FROM 
	{{ source(
        'minio',
        'vendor_books_parquet'
    ) }}
{% if is_incremental()%}
    WHERE current_timestamp > (SELECT MAX(reference_date) FROM {{ this }})
{% endif %}
