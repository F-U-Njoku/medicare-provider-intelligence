{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {# This forces dbt to use YOUR exact name instead of a prefix #}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}