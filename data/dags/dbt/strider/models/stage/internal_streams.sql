{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key='stream_id',
    database = 'iceberg',
    tags = ["internal"]
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "movie_title",
        "user_email",
        "start_at",
        "end_at"
    ]) }} AS stream_id,
    movie_title,
    user_email,
    size_mb,
    start_at,
    end_at,
    current_timestamp as reference_date
FROM
    {{ source(
        'minio',
        'internal_streams_parquet'
    ) }}
{% if is_incremental()%}
    WHERE current_timestamp > (SELECT MAX(reference_date) FROM {{ this }})
{% endif %}