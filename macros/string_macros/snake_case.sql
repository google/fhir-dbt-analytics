{% macro snake_case(str) %}
    {#- Add underscore before capital letters which follow lowercase letters -#}
    {% set str = modules.re.sub("([a-z])([A-Z])", "\\1_\\2", str) %}
    {#- Lower case the string -#}
    {{ return(str | lower) }}
{% endmacro %}