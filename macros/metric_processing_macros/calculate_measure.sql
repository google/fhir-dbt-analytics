{% macro calculate_measure(metric_definition_alias="D", metric_alias="M") -%}
   CASE
    WHEN {{metric_definition_alias}}.calculation = 'COUNT'
    THEN SUM({{metric_alias}}.measure)
    WHEN {{metric_definition_alias}}.calculation IN ('PROPORTION', 'RATIO')
    THEN SUM({{metric_alias}}.numerator)/NULLIF(SUM({{metric_alias}}.denominator),0)
    WHEN {{metric_definition_alias}}.calculation = 'DISTRIBUTION'
    THEN NULL
    ELSE {{ error(concat(["'Unknown calculation:'", metric_definition_alias~".calculation"])) }}
   END
{%- endmacro %}