{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key='user_id',
    database = 'iceberg',
    tags = ["internal"]
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        "first_name",
        "last_name"
    ]) }} AS user_id,
    first_name,
    last_name,
    CONCAT(
        first_name,
        ' ',
        last_name
    ) AS full_name,
    {{ mask_email('email') }} AS email,
    current_timestamp as reference_date 
FROM
    {{ source(
        'minio',
        'internal_users_parquet'
    ) }}
{% if is_incremental()%}
    WHERE current_timestamp > (SELECT MAX(reference_date) FROM {{ this }})
{% endif %}