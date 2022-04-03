{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {% if target.name == 'dev' %}
        {% set prefix = target.user %}
    {% else %}
        {% set prefix = none %}
    {% endif %}
    {% set final_schema = default_schema %}
    {%- if custom_schema_name is not none -%}
            {% set final_schema = custom_schema_name | trim %}
    {%- endif -%}
    {% if prefix is not none %}
        {% set final_schema =  prefix ~ '_' ~ final_schema %}
    {% endif %}

    {{ final_schema }}

{%- endmacro %}