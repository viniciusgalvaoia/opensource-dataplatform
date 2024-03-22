{{ config(
    materialized = 'table',
    database = 'iceberg',
    tags = ["internal"]
) }}

SELECT
    movie_title,
    {{ mask_email('user_email') }} AS user_email,
    size_mb,
    CAST(
        start_at AS TIMESTAMP
    ) AS start_at,
    CAST(
        end_at AS TIMESTAMP
    ) AS end_at
FROM
    {{ source(
        'minio',
        'internal_streams_parquet'
    ) }}
