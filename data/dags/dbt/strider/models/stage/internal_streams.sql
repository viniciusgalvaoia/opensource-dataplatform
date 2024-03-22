{{ config(
    materialized = 'table',
    database = 'iceberg',
    tags = ["internal"]
) }}

SELECT
    movie_title,
    user_email,
    size_mb,
    start_at,
    end_at
FROM
    {{ source(
        'minio',
        'internal_streams_parquet'
    ) }}
