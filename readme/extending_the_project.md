# Extending the project

#### [Getting Started](../README.md) &nbsp; | &nbsp; [Project overview](project_overview.md) &nbsp; | &nbsp; **Extending the project** &nbsp; | &nbsp; [Feedback](http://www.google.com/url?sa=D&q=https://docs.google.com/forms/d/e/1FAIpQLScU0WXCXA7xOX7kGr6QSW9BNMZwHswf5zq10MfRnnZJYQ6L8g/viewform)

---

This project provides the groundwork for analytics over FHIR resources stored in BigQuery.

We provide out-of-the-box data quality metrics to enable users to quickly get up-and-running with assessing the quality of their FHIR data. You will most likely have your own requirements for data quality and other analytics that you wish to run over your data. The project is designed to enable such extensions, by making use of the common building blocks contained within foundational models and macros.

Once you have a good understanding of the [project structure](https://github.com/google/fhir-dbt-analytics/blob/master/docs/project_overview.md), please refer to the documentation below for guidance on adding different components to your project.

## Add a new metric

### Instructions

1. Create a new empty SQL file within the `models/metrics/` folder, assigning it the name of your new metric. For example, *my_new_metric.sql*

1. Copy the contents of the metric template below into this new file. Alternatively, you can copy the contents of an existing metric if you prefer to adapt a working example.

1. Edit the contents of the file to adapt it to your desired metric:

    a. Update the config block with metadata for your metric.

      - Consult with the column descriptions for the *metric_definition* table within `metadata_config.yml` to understand what should be recorded within metadata fields. This information can also be viewed in the [dbt docs](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/commands/cmd-docs) site.

    b. Update the SQL query within the `metric_sql` variable:
      
      - This query will be executed over your [FHIR data](http://www.google.com/url?sa=D&q=https://hl7.org/FHIR/resourcelist.html) based on the [SQL-on-FHIR](http://www.google.com/url?sa=D&q=https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md) schema.
      - The query can be flexible, joining to any number of FHIR resources and including subqueries and common table expressions.
      - The final output from this query must contain:
        - An `id` field representing a FHIR resource id
        - The `metric_common_dimensions()` macro to derive dimensions common to all metrics
        - Any fields to be used for specific metric dimensions (as defined in the config block)
        - Any fields to be used for numerator and denominator calculations
      - The `calculate_metric` macro takes `metric_sql` as an input to produce the final metric output.
        - For count metrics, only the *metric_sql* argument is required and a distinct count over the `id` field is performed.
        - For proportion and ratio metrics, you need to provide a SQL expression to calculate the *numerator* and *denominator* fields.

1. Test running your new metric by running the following command in the project directory:
`dbt run --select my_new_metric`

1. If the metric runs successfully, check the calculated outputs in your target BigQuery schema. A new table should be created named after your metric.

### Metric template

```sql
{{ config(
   meta = {
     "description": "<TODO: Plain English description of the metric>",
     "short_description": "<TODO: Shortened version of the metric description for display in tables>",
     "primary_resource": "<TODO: FHIR resource that this metric is calculated over (e.g. AllergyIntolerance)>",
     "primary_fields": ['<TODO: Primary FHIR fields that this metric is calculated over>'],
     "secondary_resources": ['<TODO: Other FHIR resources that this metric is calculated over>'],
     "calculation": "<TODO: Type of calculation performed from: COUNT; PROPORTION; RATIO>",
     "category": "<TODO: Category to assign to this metric>",
     "metric_date_field": "<TODO: FHIR field used for the metric_date field (e.g. Encounter.period.start)>",
     "metric_date_description": "<TODO: Plain english description of the metric date (e.g. *Encounter start date)>",
     "dimension_a": "<TODO: 1st metric segmentation dimension that the metric will be grouped by>",
     "dimension_a_description": "<TODO: Description of 1st metric segmentation group>",
     "dimension_b": "<TODO: 2nd metric segmentation dimension that the metric will be grouped by>",
     "dimension_b_description": "<TODO: Description of 2nd metric segmentation group>",
     "dimension_c": "<TODO: 3rd metric segmentation dimension that the metric will be grouped by>",
     "dimension_c_description": "<TODO: Description of 3rd metric segmentation group>",
   }
) -}}

{%- set metric_sql -%}
    SELECT
      id,
      {{- metric_common_dimensions() }}
      <TODO: Derive fields for numerator, denominator and dimensions>
    FROM {{ ref('<TODO: FHIR resource (e.g. AllergyIntolerance)>') }}
{%- endset -%}

{{ calculate_metric(
    metric_sql,
    numerator = '<TODO: Only required if proportion or ratio calculation>',
    denominator = '<TODO: Only required if proportion or ratio calculation>''
) }}

```

### Tips

- The input tables for metrics are FHIR resource common table expressions (CTEs) defined within the `models/fhir_resource/` folder. They are referenced in metrics by using the [dbt ref](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/dbt-jinja-functions/ref) function. Example: `SELECT * FROM {{ref('AllergyIntolerance')}}`.

- Once a metric has been added to your project it will be automatically integrated into the data pipeline. When running the post_processing dbt models (`dbt run --selector post_processing`) the new metric will be incorporated into the `metric` table and therefore appear within _metric_views_ and any downstream data visualizations that take these views as inputs.

- Metrics can be segmented by up to three dimensions to enable drill-down analysis. To add a dimension, derive the column in the output table produced by `metric_sql` and assign an alias matching the expression provided in one of the dimension fields in the config block. If you have fewer than three dimensions, omit the unused dimensions from the config block.

- To enable time-series analysis, metrics are segmented by date where possible. This date should be the most clinically-relevant local calendar date extracted from the FHIR data. The `metric_date` field is added to FHIR resources in the FHIR resource views defined within the `models/fhir_resource/` folder. You therefore do not need to derive this date yourself within the metric SQL.

- You can use the macros in the `macros/fhir_analysis_macros/` folder to help analyze your FHIR data. For example, you can extract clinical codes from FHIR codeable concept fields using  `{{ try_code_from_codeableconcept(field_name, code_system) }}`.


## Add a new patient cohort

### Instructions

1. Create a new empty SQL file within the `models/cohorts/` folder, assigning it the name of your new cohort. For example, _my_new_cohort.sql_.

1. Copy the contents of the cohort template below into this new file. Alternatively, you can copy the contents of an existing cohort if you prefer to adapt a working example.

1. Edit the contents of the file to adapt it to your desired cohort:

    a. Update the config block with a description for your cohort.

    b. Update the SQL query WHERE clause with inclusion and exclusion criteria for your patient cohort

1. Test running the _patient_count_ metric over your new cohort by running the following command in the project directory:
`dbt run --select patient_count --vars 'cohort: my_new_cohort'`

1. If the metric runs successfully, check the calculated outputs in the `patient_count` table within your target BigQuery schema.

### Cohort template

```sql
{{- config(
   materialized = 'ephemeral',
   meta = {
     "cohort_description": "<TODO: Description of the patient cohort>"
     }
) -}}
 
SELECT
 '{{this.name}}' AS cohort_name,
 {{ get_snapshot_date() }} AS cohort_snapshot_date,
 P.id AS patient_id
FROM {{ ref('Patient_view') }} AS P
WHERE <TODO: Add criteria to filter your patient cohort list>
```

### Tips

- You can use the macros in the `macros/cohort_macros/` folder to help construct your patient cohort. For example, you can restrict to adults by using `{{ age() }} >= 18` or patients with a specified condition using `{{ has_condition('condition_name')`.

- Cohort macros join to the `clinical_code_group` table to obtain codes mapped to a clinical group (for example, a condition, procedure or medication group). This table is generated from `clinical_code_group.csv` by running the `dbt seed` command. Example clinical code groups are included in this file for demonstration purposes only. To construct cohorts with criteria based on clinical codes, update this file with the required mappings.