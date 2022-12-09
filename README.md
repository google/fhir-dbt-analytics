# FHIR dbt analytics

A dbt project which produces data-quality analytics from FHIR resources stored in BigQuery.

Use the metrics in **fhir-dbt-analytics** to check the quality of clinical data. The metrics might count the number of [FHIR resources](http://www.google.com/url?sa=D&q=http://build.fhir.org/resourcelist.html) to compare to expected counts or check references between FHIR resources, such as between patients and encounters. Some metrics can help you check the distribution of coded values in your data. You can run all the metrics as a suite, selected metrics, or individually.

Many of the metrics also break down results into different dimensions. For example, the *encounter_count* metric can show counts for different encounter classes (e.g. inpatient, emergency, ambulatory). The project includes the following elements:

- built-in metrics (parameterized so you can easily extend them) to measure clinical data quality
- views which aggregate the results ready for your data-visualization tools

You need to run these analytics tools using [dbt Core](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/docs/introduction) — an open-source data-transformation application. If you’re already analyzing FHIR data with dbt, you can take advantage of the macros from this project. The dbt macros can help you build patient cohorts, navigate and extract values from FHIR resources, or inspect BigQuery datasets. The dbt selectors gather metrics into themes so that you can run just the metrics you’re interested in.


## What you'll need

Before you can run this project, you’ll need the following:

- [dbt BigQuery adapter](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup) 1.2.0+ installed on your computer
- A [Google Cloud project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) where you have `bigquery.dataEditor` and `bigquery.user` permissions
- The [gcloud](https://cloud.google.com/sdk/docs/install) command line interface for [authentication](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#local-oauth-gcloud-setup)


## Install the project

To install the project, run the following commands in your terminal to create a new folder in the current directory:

```
git clone https://github.com/google/fhir-dbt-analytics
cd fhir-dbt-analytics
```


## Setup dbt outputs

Open `profiles.yml` and fill in the project and dataset as indicated in the file.


## Setup source data

By default, the source data are from the [Synthea Generated Synthetic Data in FHIR](https://console.cloud.google.com/marketplace/details/mitre/synthea-fhir) public dataset. You can test running the project over this dataset by leaving the defaults unchanged.

To analyze your own data, export them to BigQuery from a Google Cloud FHIR store, following [Storing healthcare data in BigQuery](https://cloud.google.com/architecture/storing-healthcare-data-in-bigquery) and [point the project variables](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/docs/build/project-variables) to it by editing the `dbt_project.yml` file:

- **database**: The name of a Google Cloud project which contains your FHIR BigQuery dataset. For example, *bigquery-public-data*.
- **schema**: The name of your FHIR BigQuery dataset. For example, *fhir_synthea*.
- **timezone_default**: The IANA time-zone name. For example, *Europe/London*.


## Run the project

### First time

The first time that you run the project, you need to [seed static data](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/docs/build/seeds) which includes the clinical codes and time zones. Run the following command in the project directory:

```
dbt seed
```

### Analytics

Now you're ready to create the analytics by running the following two commands in your terminal:

```
dbt run
dbt run --selector post_processing
```

The `dbt run` command runs all the data quality metrics in the project. To save time, you can run a selection of metrics if you include a selector argument from [selectors.yml](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/node-selection/yaml-selectors). For example, to run only the Encounter metrics, use `dbt run --selector resource_encounter`.

`dbt run --selector post_processing` runs models that consolidate the metric outputs.


## Project overview

### Metric data model

This project implements a standardized data model for aggregate data quality
metrics executed over FHIR data.

This model includes:

1.  A consistent format for metric outputs
1.  A consistent format and definitions for metric metadata
1.  Formalized concepts such as metric date, metric category, metric slice, site
    and source system

##### Metric output

The SQL queries for all metrics (located within the `models/metrics/` folder)
produce an output table with the same fields. This consistent format
allows outputs to be unioned into a single table, which enables common
downstream handling.

Field definitions are recorded as column descriptions for the `metric` model in `models/post_processing/post_processing_config.yml`. This can also be viewed in the [dbt docs](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/commands/cmd-docs) site.

##### Metric metadata

All metrics added to the `models/metrics` folder must have appropriate metadata
documented. This metadata serves the following purposes:

1.  Aid user interpretation of metrics by surfacing clear metric descriptions and definitions.
1.  Enable automated manipulation of metrics. For example, correctly aggregate
    count and proportion metrics.
1.  Enable organization and retrieval of metrics. For example, return metrics
    that evaluate a specific FHIR resource.

In this project, the convention is to record metric metadata as
[model configurations](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/model-configs)
within config blocks at the top of each metric model.

Metadata field definitions are recorded as column descriptions for the `metric_definition` model in `models/metadata/metadata_config.yml`. This can also be viewed in the [dbt docs](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/commands/cmd-docs) site.


### Models

This project organizes dbt models into the following folders. Data Quality
metrics are located within the `models/metrics/data_quality` folder. All other folders
contain SQL scripts for data transformations before or after the execution of
metrics.

##### fhir_resources

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

##### metadata

dbt models that write metadata to the database, such as project variables and metric metadata.

##### metrics

All metrics with one dbt model per metric. Metrics are structured in accordance with the metric data model.

The input tables for metrics are FHIR resource common table expressions (CTEs) defined within the `fhir_resource` folder. They are referenced in metrics by using the [dbt ref](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/dbt-jinja-functions/ref) function. For example: `SELECT * FROM {{ref(AllergyIntolerance)}}`

##### post_processing

dbt models that further transform the outputs from executing the data quality metrics. The `metric` table contains the latest data for each metric that has been executed.

##### metric_views

SQL views that join the metric outputs with their metadata and groups them by varying levels of aggregation. Data visualization tools can read from these views to display the metrics.

##### cohorts

dbt models for constructing patient cohorts that can be used in analyses. Each dbt model generates a common table expression (CTE) for one patient cohort. Cohorts can be constructed by using macros within the `macros/cohort_macros` folder.

Metrics can be selectively run against a patient cohort by setting the  `cohort` project variable to the name of the cohort. For example: `dbt run --vars 'cohort: adults'`.


### Macros

[Macros](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/docs/building-a-dbt-project/jinja-macros) are a powerful tool within dbt projects that allow writing modular SQL that utilize control structures and variables via Jinja templating.

Macros are organized into the following folders:

- `cohort_macros`: Extract patient characteristics to construct patient cohorts
- `fhir_analysis_macros`: Commonly used complex SQL to analyze FHIR data
- `fhir_view_macros`: Construct views and CTEs for FHIR resources
- `infrastructure_macros`: Interact with BigQuery and dbt objects
- `metric_processing_macros`: Process metric output tables
- `string_macros`: Transform and format string values
- `data_visualization_macros`: Connect metric outputs to data visualization tools


### Selectors

[dbt selectors](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/node-selection/yaml-selectors) allow complex selection criteria to be defined for which dbt models to run.

In this project a default set of selectors are defined for logical groupings of metrics by FHIR resource and metric category. Users can extend this by defining their own selectors.


### Tests

[dbt tests](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/docs/building-a-dbt-project/tests) are assertions made about the models and other resources in the dbt project.

This project uses tests to ensure all metrics added to the library comply with the structure defined by our data model and have required metadata recorded.


### Data visualization

Running this dbt project writes outputs to BigQuery tables and views within your target *project* and *dataset*, as defined in [profiles.yml](http://www.google.com/url?sa=D&q=https://docs.getdbt.com/reference/profiles.yml).

These tables and views can be connected to a data visualization tool to display and investigate the metrics. A template [Looker Studio](https://cloud.google.com/looker-studio) dashboard is available that you can clone and connect to your data.

##### Clone Looker Studio dashboard

1.  Copy the URL within the `looker_studio_url` column of the `project_variables` table written to your dataset
1.  Paste the URL into a web browser
1.  Click the blue "Edit and share" button in the top-right corner
1.  Review the data sources and then click "Acknowledge and save" to save your copy of this dashboard


## Support

fhir-dbt-analytics is not an officially supported Google product. If you believe that something’s not working, please [create a GitHub issue](http://www.google.com/url?sa=D&q=https://docs.github.com/en/issues/tracking-your-work-with-issues/creating-an-issue).