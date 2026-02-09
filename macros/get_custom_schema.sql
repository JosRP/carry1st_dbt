{% macro generate_schema_name(custom_schema_name, node) -%}
    {# If a custom schema is provided (like in dbt_project.yml), use it exactly #}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {# Otherwise, use the default schema from your profile #}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}