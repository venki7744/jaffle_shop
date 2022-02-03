{% macro get_create_table_like_sql(temporary, source, relation) -%}
  {{ adapter.dispatch('get_create_table_like_sql', 'dbt')(temporary, source, relation) }}
{%- endmacro %}

{% macro default__get_create_table_like_sql(temporary, source,relation) -%}
  {{ return(create_table_as(temporary, source, relation)) }}
{% endmacro %}


/* {# keep logic under old macro name for backwards compatibility #} */
{% macro create_table_like(temporary, source, relation) -%}
  {{ adapter.dispatch('create_table_like', 'dbt')(temporary, source, relation) }}
{%- endmacro %}

{% macro default__create_table_like(temporary, source, relation) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set add_columns = config.get('add_columns', none) -%}
  {{ sql_header if sql_header is not none }}
  
  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  like (
    {{ source.include(database=(not temporary), schema=(not temporary))  }}
  );

  {% if add_columns is not none %}
    {% for col in add_columns %}
      alter table 
        {{ relation.include(database=(not temporary), schema=(not temporary)) }} add column
        {{ col["name"] }} {%if col["type"] %}col["type"]{%- endif %} {% if col["default"] %} {% col["default"] {% endif %}{% if col["xtra_attrb"] %} col["xtra_attrb"] {% endif %}
        ;
    {% endfor %}
  {% endif %}  
  {% set cols = adapter.get_columns_in_relation(source) %}
  
  insert into 
    {{ relation.include(database=(not temporary), schema=(not temporary)) }} 
    (
     {% for col in cols %}
      {{col}}
      {% if not loop.last %} , {% endif %}
     {% endfor %} 
    )
    select 
    
     {% for col in cols %}
      {{col}}
      {% if not loop.last %} , {% endif %}
     {% endfor %} 
    from
     {{ source.include(database=(not temporary), schema=(not temporary)) }} 
    ;
  
{%- endmacro %}