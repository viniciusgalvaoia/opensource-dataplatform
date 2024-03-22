{{ config(
    materialized = 'table',
    database = 'iceberg',
    tags = ["internal"]
) }}

SELECT
    title,
    duration_mins,
    original_language,
    size_mb
FROM
    {{ source(
        'minio',
        'internal_movies_parquet'
    ) }}
