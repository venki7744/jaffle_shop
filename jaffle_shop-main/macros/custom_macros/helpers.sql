{% macro to_clean_string(obj) %}
    {% set clear_string = obj | string | replace('\\n',' ') | replace("'","\\'") | replace("\\\\","\\") | trim %}
   {{ return(clear_string) }}
{% endmacro %}