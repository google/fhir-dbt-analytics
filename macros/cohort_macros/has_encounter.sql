{% macro has_encounter(class=None, lookback=None) -%}
 EXISTS (
  SELECT E.id
  FROM {{ ref('Encounter_view') }} AS E
  WHERE P.id = E.subject.patientId
  AND E.status IN ('in-progress', 'finished')
  {%- if class != None %}
  AND class.code {{ sql_comparison_expression(class) }}
  {%- endif %}
  AND DATE(E.period.start) <= {{ get_snapshot_date() }}
  {%- if lookback != None %}
  AND DATE(E.period.start) >= {{ get_snapshot_date() }} - INTERVAL {{ lookback }}
  {%- endif %}
)
{%- endmacro %}