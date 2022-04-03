{% macro log_job_start() -%}
  {{ return(adapter.dispatch('log_job_start')()) }}
{%- endmacro %}

{% macro default__log_job_start() -%}
    {% set audit_db = config.get('audit_db') %}
    {% set audit_schema = generate_schema_name(config.get('audit_schema'),node) %}
    {% set full_qualified_schema = audit_db + '.' + audit_schema | trim %}
    {% do create_custom_schema(full_qualified_schema) %}
    {%set run_sql %}
        select * from {{audit_db}}.{{audit_schema}}.run_log_header 
        where run_id =  {{ var('restart_id', -1) }} 
    {% endset %}
    
  {% if execute %}
    {{ log("run_sql1 " ~ run_sql )}}
    {% set result = run_query(run_sql) %}
    {% set run_metadata %}
        {"project_name":{{project_name }},"dbt_version": {{dbt_version }} }
    {% endset %}
    {% if not result %}
        /*{%set run_sql_insert %}*/
        insert into {{audit_db}}.{{audit_schema}}.run_log_header (invoke_id, user_id, run_start, 
        run_end, status, run_metadata)
         values (
             '{{invocation_id }}',
             '{{ target.user }}',
             '{{ run_started_at }}',
             null,
             'started',
             '{{ run_metadata }}'
         );
         commit;
         /*{% endset %}
         {{ log("run_sql2 " ~ run_sql_insert )}}
         /* {% do run_query(run_sql) %} 
         {{ run_sql_insert }}*/
    {% else %}
        select 1 ;
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro log_job_end(run_results) -%}
  {{ return(adapter.dispatch('log_job_end')(run_results)) }}
{%- endmacro %}

{% macro default__log_job_end(run_results) -%}
    {% set audit_db = config.get('audit_db') %}
    {% set audit_schema = generate_schema_name(config.get('audit_schema'),node) %}
    {% set full_qualified_schema = audit_db + '.' + audit_schema | trim %}
    {% do create_custom_schema(full_qualified_schema) %}
    {%set run_sql %}
        select run_id from {{audit_db}}.{{audit_schema}}.run_log_header 
        where invoke_id = '{{ invocation_id }}'
        order by run_id desc limit 1;
    {% endset %}
    {% if execute %}
        {{ log("run_sql3 " ~ run_sql )}}
        {% set run_id_list = run_query(run_sql) %}
        {% if run_id_list is none %}
            {% set msg %}
                run ID cannot be none at job log end.
            {% endset %}
        {% do exceptions.raise_compiler_error(msg) %}
        {% endif %}
        {{ log("run_id_list" ~ run_id_list[0]) }}
        {% set run_id = run_id_list[0]['RUN_ID'] %}
        {% set run_status = "success" %}
        {% set run_metadata = [] %}
        {{ log("run_results: " ~ run_results )}}
        {% for res in run_results %}
            {% if res.status != "success" %}
                {% set run_status = "fail" %}
            {% endif %}
            {% set timing = res.timing %}
                {% set step_start %}{{ timing[1].started_at }}{% endset %}
            {% set step_end %}{{ timing[1].completed_at }}{% endset %}
            {% set unique_id %}{{ to_clean_string(res.node.unique_id) }}{% endset %}
            {% set status = res.status %}
            {% set step_metadata %}{{ to_clean_string(res) }}{% endset %}
            {% set log_details_update %}
                update {{audit_db}}.{{audit_schema}}.run_log_details 
                    set 
                    step_start = '{{ step_start }}',
                    step_end = '{{ step_end }}',
                    status = '{{ status }}',
                    step_metadata = '{{ step_metadata }}'
                where run_id = {{ run_id }}
                and step_id = '{{ unique_id }}'
                and version_id = (select max(version_id) VERSION_ID from 
                {{audit_db}}.{{audit_schema}}.run_log_details 
                where run_id = {{ run_id }}
                and step_id = '{{ unique_id }}');
            {% endset %}
            {% do run_query(log_details_update) %}
            {% do run_metadata.append(res) %}
        {% endfor %}
        {% set run_metadata_string = to_clean_string(run_metadata) %}
        {% set run_sql_update %}
            update {{audit_db}}.{{audit_schema}}.run_log_header
            set run_end = {{ dbt_utils.current_timestamp() }},
            status = '{{ run_status }}',
            run_metadata = '{{ run_metadata_string }}'
            where run_id = {{ run_id }};
        {% endset %}
        {{ log("run_sql4 " ~ run_sql_update )}}
        /* {% do run_query(run_sql) %} */
        {{ run_sql_update }} 
    {% else %}
        {{ " " }}
    {% endif %}
{% endmacro %}

{% macro log_job_step(method,  Result) -%}
  {{ adapter.dispatch('log_job_step')(method,  Result) }}
{%- endmacro %}

{% macro default__log_job_step(method, Result) -%}
    {% set audit_db = config.get('audit_db') %}
    {% set audit_schema = generate_schema_name(config.get('audit_schema'),node) %}
    {% set full_qualified_schema = audit_db + '.' + audit_schema | trim %}
    {% do create_custom_schema(full_qualified_schema) %}
    {%set run_sql %}
        select run_id from {{audit_db}}.{{audit_schema}}.run_log_header 
        where invoke_id = '{{ invocation_id }}'
        order by run_id desc limit 1;
    {% endset %}
    {% if execute %}
        {{ log("run_sql6 " ~ run_sql )}}
        {% set run_id_list = run_query(run_sql) %}
        {% set new_run_id = run_id_list[0]['RUN_ID'] %}
        {% set run_id =  var('restart_id', new_run_id) %} 
        {% set version_id = 0 %}
        {% set step_id = model.unique_id %}
        {% set step_start = dbt_utils.current_timestamp() %}
        {% set status = method %}
        {% set step_metadata = '' %}
        {% set get_version_id %}
                select max(version_id) as VERSION_ID from {{audit_db}}.{{audit_schema}}.run_log_details 
                where run_id = {{ run_id }}
                and step_id = '{{ step_id }}'
        {% endset %}
        {{ log("run_sql7 " ~ get_version_id )}}
        {% set version_id_result = run_query(get_version_id) %}
        {% set version_id = 0 %}
        {% if version_id_result[0]['VERSION_ID'] is not none %}
            {% set version_id = version_id_result[0]['VERSION_ID'] %}
        {% endif %}
        {{ log("version_id " ~ version_id )}}
        {% if method == 'exit' %}
            {% set step_end = dbt_utils.current_timestamp() %}
            {% set status = 'end' %}

            {% set update_sql %}
                update {{audit_db}}.{{audit_schema}}.run_log_details 
                set
                    step_end = {{ step_end }},
                    status = '{{ status }}',
                    step_metadata = '{{ step_metadata }}'
                where run_id = {{ run_id }}
                and step_id = '{{ step_id }}'
                and version_id = {{ version_id }}
            {% endset %}
            {{ log("run_sql8 " ~ update_sql )}}
            {{update_sql}}
        {% else %}
            {% set check_row %}
                select 1 as CHECK_RECORD from {{audit_db}}.{{audit_schema}}.run_log_details 
                where run_id = {{ run_id }}
                and step_id = '{{ step_id }}'
            {% endset %}
            {{ log("run_sql9 " ~ check_row )}}
            {% set check_result = run_query(check_row) %}
            {% if check_result is not none %}
                {% set version_id = version_id + 1 %}
            {% endif %}
            {% set insert_log_sql %}
                insert into {{audit_db}}.{{audit_schema}}.run_log_details
                values (
                    {{ run_id }},
                    '{{ step_id }}',
                    {{ version_id }},
                    {{ step_start }},
                    null,
                    '{{ status }}',
                    '{{ step_metadata }}'
                )
            {% endset %}
            {{ log("run_sql10 " ~ insert_log_sql )}}
            {{ insert_log_sql }}

        {% endif %}
    {% endif %}
{% endmacro %}