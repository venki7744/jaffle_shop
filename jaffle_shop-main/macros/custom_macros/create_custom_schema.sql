{% macro create_custom_schema(full_schema_name) -%}
  {{ adapter.dispatch('create_custom_schema','dbt')(full_schema_name) }}
{% endmacro %}

{% macro default__create_custom_schema(full_schema_name) -%}
  {%- call statement('create_custom_schema') -%}
    create schema if not exists {{ full_schema_name }}
  {% endcall %}
{% endmacro %}