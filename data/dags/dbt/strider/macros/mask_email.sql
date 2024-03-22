{% macro mask_email(email) %}
    CASE
        WHEN {{ email }} IS NULL THEN NULL
        ELSE CONCAT(SUBSTRING(email, 1, 1), '*****', SUBSTRING(email, strpos(email, '@')))
    END
{% endmacro %}