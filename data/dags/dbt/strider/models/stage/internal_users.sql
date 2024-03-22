{{ config(
    materialized = 'table',
    database = 'iceberg',
    tags = ["internal"]
) }}

SELECT
    first_name,
    last_name,
    CONCAT(
        first_name,
        ' ',
        last_name
    ) AS full_name,
    {{ mask_email('email') }} AS email 
FROM
    {{ source(
        'minio',
        'internal_users_parquet'
    ) }}
