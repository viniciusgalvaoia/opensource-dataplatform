{{ config(
    materialized = 'table',
    database = 'iceberg',
    tags = ["vendor"]
) }}


SELECT 
	name,
    pages,
    author,
    publisher
FROM 
	{{ source(
        'minio',
        'vendor_books_parquet'
    ) }}
    
