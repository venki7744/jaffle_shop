{% macro get_merge_sql(target, source, unique_key, dest_columns, predicates=none) -%}
  {{ adapter.dispatch('get_merge_sql', 'dbt')(target, source, unique_key, dest_columns, predicates) }}
{%- endmacro %}

{% macro default__get_merge_sql(target, source, unique_key, dest_columns, predicates) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set add_columns = config.get('add_columns', none) -%}
    {% set add_cols = [] %}
    {% set dest_cols_list = [] %}
    {% for column in add_columns %}
        {% do add_cols.append(column['name'])  %}
    {% endfor %}
    {% for dest_col in dest_cols_csv.split(',') %}
        {% if dest_col not in add_cols %}
            {% do dest_cols_list.append(dest_col) %}
        {% endif %}
    {% endfor %}
    {% set dest_cols_csv = ",".join(dest_cols_list) %}
    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set unique_key_match %}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
            {% endset %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }}

    {% if unique_key %}
    when matched and
    (
         {% for column_name in update_columns -%}
             DBT_INTERNAL_DEST.{{ column_name }} <> DBT_INTERNAL_SOURCE.{{ column_name }}
            {% if not loop.last %} OR  {% endif %}
        {%- endfor %}
    )
    
    then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}


{% macro get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}
  {{ adapter.dispatch('get_delete_insert_merge_sql', 'dbt')(target, source, unique_key, dest_columns) }}
{%- endmacro %}

{% macro default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {% set add_cols = [] %}
    {% set dest_cols_list = [] %}
    {% for column in add_columns %}
        {% do add_cols.append(column['name'])  %}
    {% endfor %}
    {% for dest_col in dest_cols_csv.split(',') %}
        {% if dest_col not in add_cols %}
            {% do dest_cols_list.append(dest_col) %}
        {% endif %}
    {% endfor %}
    {% set dest_cols_csv = ",".join(dest_cols_list) %}

    {% if unique_key is not none %}
    delete from {{ target }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ source }}
    );
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}


{% macro get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header=false) -%}
  {{ adapter.dispatch('get_insert_overwrite_merge_sql', 'dbt')(target, source, dest_columns, predicates, include_sql_header) }}
{%- endmacro %}

{% macro default__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {% set add_cols = [] %}
    {% set dest_cols_list = [] %}
    {% for column in add_columns %}
        {% do add_cols.append(column['name'])  %}
    {% endfor %}
    {% for dest_col in dest_cols_csv.split(',') %}
        {% if dest_col not in add_cols %}
            {% do dest_cols_list.append(dest_col) %}
        {% endif %}
    {% endfor %}
    {% set dest_cols_csv = ",".join(dest_cols_list) %}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none and include_sql_header }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
        {% if predicates %} and {{ predicates | join(' and ') }} {% endif %}
        then delete

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}