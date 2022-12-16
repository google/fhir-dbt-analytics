{%- macro fhir_resource_table_expression() -%}
    {%- if execute -%}
      {%- set fhir_resource = model_metadata(meta_key='fhir_resource') -%}
      {%- set fhir_resource_view = fhir_resource~"_view" -%}
      {%- set patient_reference_column = model_metadata(meta_key='patient_reference_column', model_name=fhir_resource_view) -%}
        SELECT * FROM {{ ref(fhir_resource_view) }}
    {%- else %}
        SELECT * FROM undefined
    {%- endif -%}
        {%- if var('cohort') != 'all_patients' and patient_reference_column != None %}
        WHERE EXISTS (
          SELECT cohort.patient_id
          FROM {{ ref(var('cohort')) }} AS cohort
          {%- if fhir_resource == 'Patient' %}
          WHERE id = cohort.patient_id
          {%- elif fhir_resource == 'Person' %}
          JOIN UNNEST(link) AS l ON l.target.patientId = cohort.patient_id
          {%- else %}
          WHERE {{patient_reference_column}}.patientId = cohort.patient_id
          {%- endif %}
        )
        {% endif -%}
{% endmacro %}