{% macro get_create_table_like_sql(temporary, target_relation, source) -%}
  {{ adapter.dispatch('get_create_table_like_sql', 'dbt')(temporary, target_relation, source) }}
{%- endmacro %}

{% macro default__get_create_table_like_sql(temporary, target_relation, source) -%}
  {{ return(create_table_like(temporary, target_relation, source)) }}
{% endmacro %}


/* {# keep logic under old macro name for backwards compatibility #} */
{% macro create_table_like(temporary, target_relation, source) -%}
  {{ adapter.dispatch('create_table_like', 'dbt')(temporary, target_relation, source) }}
{%- endmacro %}

{% macro snowflake__create_table_like(temporary, target_relation, source) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set add_columns = config.get('add_columns', none) -%}
  {%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
  {%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}
  {%- set copy_grants = config.get('copy_grants', default=false) -%}

  {%- if cluster_by_keys is not none and cluster_by_keys is string -%}
      {%- set cluster_by_keys = [cluster_by_keys] -%}
  {%- endif -%}
  {%- if cluster_by_keys is not none -%}
      {%- set cluster_by_string = cluster_by_keys|join(", ")-%}
  {% else %}
      {%- set cluster_by_string = none -%}
  {%- endif -%}
  {{ sql_header if sql_header is not none }}
  
  create or replace {% if temporary: -%}temporary{%- endif %} table 
    {{ target_relation.include(database=(not temporary), schema=(not temporary)) }}
  like 
    {{ source.include(database=(not temporary), schema=(not temporary))  }}
  {% if cluster_by_string is not none and not temporary %} cluster by({{ cluster_by_string }}) {% endif %}
  {% if not temporary and copy_grants is not none %} COPY GRANTS {% endif %}
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
     {% if cluster_by_string is not none and not temporary %} order by {{ cluster_by_string }} {% endif %}
    ;
  
{%- endmacro %}