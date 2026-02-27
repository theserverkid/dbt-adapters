{% materialization view, adapter='iceberg' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
