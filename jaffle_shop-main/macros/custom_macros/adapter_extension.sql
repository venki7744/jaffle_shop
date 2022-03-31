{% macro get_columns_in_relation_except(relation) -%}
  {{ adapter.dispatch('get_columns_in_relation_except', 'dbt')(relation) }}
{%- endmacro %}

{% macro snowflake__get_columns_in_relation_except(relation) -%}
  {%- set sql -%}
    describe table {{ relation }}
  {%- endset -%}
  {%- set result = run_query(sql) -%}

  {% set maximum = 10000 %}
  {% if (result | length) >= maximum %}
    {% set msg %}
      Too many columns in relation {{ relation }}! dbt can only get
      information about relations with fewer than {{ maximum }} columns.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}
  {{ log("in: get_columns_in_relation_except") }}
  {% set columns = [] %}
  {% set add_columns = config.get("add_columns", none) %}
  {% set add_cols = [] %}
  {% for column in add_columns %}
      {% do add_cols.append(column['name'])  %}
  {% endfor %}
  {{ log("describe return:"~result) }}
  {% for row in result %}
    {% if row['name'] not in add_cols %}
        {% do columns.append(api.Column.from_description(row['NAME'], row['TYPE'])) %}
    {% endif %}
  {% endfor %}
  {{ log("cols in rel except:"~columns) }}
  {% do return(columns) %}
{% endmacro %}


{% macro redshift__get_columns_in_relation_except(relation) -%}
  {%- set sql -%}
    Select * from svv_columns where table_schema = {{ relation.schema }} 
     and table_name = {{ relation.identifier }} 
  {%- endset -%}
  {%- set result = run_query(sql) -%}

  {% set maximum = 10000 %}
  {% if (result | length) >= maximum %}
    {% set msg %}
      Too many columns in relation {{ relation }}! dbt can only get
      information about relations with fewer than {{ maximum }} columns.
    {% endset %}
    {% do exceptions.raise_compiler_error(msg) %}
  {% endif %}
  {{ log("in: get_columns_in_relation_except") }}
  {% set columns = [] %}
  {% set add_columns = config.get("add_columns", none) %}
  {% set add_cols = [] %}
  {% for column in add_columns %}
      {% do add_cols.append(column['name'])  %}
  {% endfor %}
  {{ log("describe return:"~result) }}
  {% for row in result %}
    {% if row['column_name'] not in add_cols %}
        {% do columns.append(api.Column.from_description(row['column_name'], row['data_type'])) %}
    {% endif %}
  {% endfor %}
  {{ log("cols in rel except:"~columns) }}
  {% do return(columns) %}
{% endmacro %}