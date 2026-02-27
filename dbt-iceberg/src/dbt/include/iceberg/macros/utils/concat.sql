{% macro iceberg__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}
