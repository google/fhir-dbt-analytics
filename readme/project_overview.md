# Project overview

#### [Getting Started](../README.md) &nbsp; | &nbsp; **Project overview** &nbsp; | &nbsp; [Extending the project](extending_the_project.md) &nbsp; | &nbsp; [Feedback](http://www.google.com/url?sa=D&q=https://docs.google.com/forms/d/e/1FAIpQLScU0WXCXA7xOX7kGr6QSW9BNMZwHswf5zq10MfRnnZJYQ6L8g/viewform)

--------------------------------------------------------------------------------

## Metric data model

This project implements a standardized data model for aggregate data quality
metrics executed over FHIR data.

This model includes:

1.  A consistent format for metric outputs
1.  A consistent format and definitions for metric metadata
1.  Formalized concepts such as metric date, metric category, metric dimensions,
    site and source system

### Metric output

The SQL queries for all metrics (located within the `models/metrics/` folder)
produce an output table with the same fields. This consistent format allows
outputs to be unioned into a single table, which enables common downstream
handling.

Field definitions are recorded as column descriptions for the `metric` model in
`models/post_processing/post_processing_config.yml`. This can also be viewed in
the
[dbt docs](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/commands/cmd-docs)
site.

### Metric metadata

All metrics added to the `models/metrics` folder must have appropriate metadata
documented. This metadata serves the following purposes:

1.  Aid user interpretation of metrics by surfacing clear metric descriptions
    and definitions.
1.  Enable automated manipulation of metrics. For example, correctly aggregate
    count and proportion metrics.
1.  Enable organization and retrieval of metrics. For example, return metrics
    that evaluate a specific FHIR resource.

In this project, the convention is to record metric metadata as
[model configurations](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/model-configs)
within config blocks at the top of each metric model.

Metadata field definitions are recorded as column descriptions for the
`metric_definition` model in `models/metadata/metadata_config.yml`. This can
also be viewed in the
[dbt docs](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/commands/cmd-docs)
site.

## Models

This project organizes dbt models into the following folders. Data quality
metrics are located within the `models/metrics/data_quality` folder. All other
folders contain SQL scripts for data transformations before or after the
execution of metrics.

### fhir_resources

Foundational models for the project that produce views and common table
expressions (CTEs) for FHIR resources that are then referenced by the metrics.

For each FHIR resource, the project contains two dbt models:

-   A View named `FhirResource_view` (e.g. *AllergyIntolerance_view*)
-   A CTE named `FhirResource` (e.g. *AllergyIntolerance*)

Logic performed within the *View*:

-   Identify whether the FHIR resource exists in the BigQuery dataset
-   Construct the view referencing the FHIR resource table
-   Append metadata commonly used by the metrics such as `metric_date`,
    `source_system` and `site`

Logic performed within the *CTE*:

-   Reference the corresponding FHIR View
-   Filter the View on the patient cohort selected by the `cohort` project
    variable (no filtering applied if `cohort=all_patients`)

### metadata

dbt models that write metadata to the database, such as project variables and
metric metadata.

### metrics

All metrics with one dbt model per metric. Metrics are structured in accordance
with the metric data model.

The input tables for metrics are FHIR resource common table expressions (CTEs)
defined within the `fhir_resource` folder. They are referenced in metrics by
using the
[dbt ref](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/dbt-jinja-functions/ref)
function. For example: `SELECT * FROM {{ref(AllergyIntolerance)}}`

### post_processing

dbt models that further transform the outputs from executing the data quality
metrics. The `metric` table contains the latest data for each metric that has
been executed.

### metric_views

SQL views that join the metric outputs with their metadata and groups them by
varying levels of aggregation. Data visualization tools can read from these
views to display the metrics.

### cohorts

dbt models for constructing patient cohorts that can be used in analyses. Each
dbt model generates a common table expression (CTE) for one patient cohort.
Cohorts can be constructed by using macros within the `macros/cohort_macros`
folder.

Metrics can be selectively run against a patient cohort by setting the `cohort`
project variable to the name of the cohort. For example: `dbt run --vars
'cohort: adults'`.

## Macros

[Macros](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/docs/building-a-dbt-project/jinja-macros)
are a powerful tool within dbt projects that allow writing modular SQL that
utilize control structures and variables via Jinja templating.

Macros are organized into the following folders:

-   `cohort_macros`: Extract patient characteristics to construct patient
    cohorts
-   `fhir_analysis_macros`: Commonly used complex SQL to analyze FHIR data
-   `fhir_view_macros`: Construct views and CTEs for FHIR resources
-   `infrastructure_macros`: Interact with BigQuery and dbt objects
-   `metric_processing_macros`: Calculate metrics and process metric output
    tables
-   `string_macros`: Transform and format string values
-   `data_visualization_macros`: Connect metric outputs to data visualization
    tools

## Selectors

[dbt selectors](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/node-selection/yaml-selectors)
allow complex selection criteria to be defined for which dbt models to run.

In this project a default set of selectors are defined for logical groupings of
metrics by FHIR resource and metric category. Users can extend this by defining
their own selectors.

## Tests

[dbt tests](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/docs/building-a-dbt-project/tests)
are assertions made about the models and other resources in the dbt project.

This project uses tests to ensure all metrics added to the library comply with
the structure defined by our data model and have required metadata recorded.

## Data visualization

Running this dbt project writes outputs to BigQuery tables and views within your
target *project* and *dataset*, as defined in
[profiles.yml](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/profiles.yml).

These tables and views can be connected to a data visualization tool to display
and investigate the metrics. A template
[Looker Studio](https://cloud.google.com/looker-studio) dashboard is available
that you can clone and connect to your data.

### Clone Looker Studio dashboard

1.  Copy the URL within the `looker_studio_url` column of the
    `project_variables` table written to your dataset.

    *   To make sure you have the entire content of the value, you can use
        command line:

        ```shell
        bq query --nouse_legacy_sql 'SELECT looker_studio_url FROM `<GCP project>.<dataset>.project_variables`'
        ```

    *   Click on the output (if clickable), or copy from console to the URL box
        in a browser.

1.  This will make a copy of the dashboard that points to your data.

1.  Click the blue *"Edit and share"* button in the top-right corner.

1.  Review the data sources and then click *"Acknowledge and save"* to save your
    copy of this dashboard.
