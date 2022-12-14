# Getting started

#### **Getting Started** &nbsp; | &nbsp; [Project overview](readme/project_overview.md) &nbsp; | &nbsp; [Extending the project](readme/extending_the_project.md) &nbsp; | &nbsp; [Feedback](http://www.google.com/url?sa=D&q=https://docs.google.com/forms/d/e/1FAIpQLScU0WXCXA7xOX7kGr6QSW9BNMZwHswf5zq10MfRnnZJYQ6L8g/viewform)

---

## What is FHIR dbt analytics?

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


## Support

fhir-dbt-analytics is not an officially supported Google product. If you believe that something’s not working, please [create a GitHub issue](http://www.google.com/url?sa=D&q=https://docs.github.com/en/issues/tracking-your-work-with-issues/creating-an-issue).