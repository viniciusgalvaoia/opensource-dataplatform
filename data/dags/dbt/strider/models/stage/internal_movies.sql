{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key='title',
    database = 'iceberg',
    tags = ["internal"]
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "title",
        "original_language"
    ]) }} AS movie_id,
    title,
    duration_mins,
    original_language,
    size_mb,
    current_timestamp as reference_date
FROM
    {{ source(
        'minio',
        'internal_movies_parquet'
    ) }}
{% if is_incremental()%}
    WHERE current_timestamp > (SELECT MAX(reference_date) FROM {{ this }})
{% endif %}
