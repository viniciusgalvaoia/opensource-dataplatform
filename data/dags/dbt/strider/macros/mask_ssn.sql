{% macro mask_ssn(ssn_column) %}
    CONCAT('xxx-xx-', SUBSTRING({{ ssn_column }}, 8, 4))
{% endmacro %}
