{% macro get_create_table_like_sql(temporary, target_relation, source) -%}
  {{ adapter.dispatch('get_create_table_like_sql', 'dbt')(temporary, target_relation, source) }}
{%- endmacro %}

{% macro default__get_create_table_like_sql(temporary, target_relation, source) -%}
  {{ return(create_table_as(temporary, source, relation)) }}
{% endmacro %}


/* {# keep logic under old macro name for backwards compatibility #} */
{% macro create_table_like(temporary, target_relation, source) -%}
  {{ adapter.dispatch('create_table_like', 'dbt')(temporary, target_relation, source) }}
{%- endmacro %}

{% macro default__create_table_like(temporary, target_relation, source) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set add_columns = config.get('add_columns', none) -%}
  {{ sql_header if sql_header is not none }}
  
  create or replace {% if temporary: -%}temporary{%- endif %} table
    {{ target_relation.include(database=(not temporary), schema=(not temporary)) }}
  like 
    {{ source.include(database=(not temporary), schema=(not temporary))  }}
  ;

  {% if add_columns is not none %}
    {% for col in add_columns %}
      alter table 
        {{ log("col :" ~ col)}}
        {{ target_relation.include(database=(not temporary), schema=(not temporary)) }} add column
        {{ col["name"] }} {%if col["type"] %} {{ col["type"] }} {% endif %} {% if col["default"] %} default {{ col["default"] }}  {% endif %}{% if col["xtra_attrb"] %} {{ col["xtra_attrb"] }} {% endif %}
        ;
    {% endfor %}
  {% endif %}  
  {% set cols = get_quoted_csv(adapter.get_columns_in_relation(source) | map(attribute='name')) %}
  
  insert into 
    {{ target_relation.include(database=(not temporary), schema=(not temporary)) }} 
    (
    {{cols}}
    )
    select 
    
     {{cols}}
    from
     {{ source.include(database=(not temporary), schema=(not temporary)) }} 
    ;
  
{%- endmacro %}