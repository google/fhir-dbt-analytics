# Superset dashboards

This folder contains a demo dashboard exported from [Apache Superset](https://superset.apache.org). It visualizes data from [Data Quality Metrics](../../README.md#data-quality-metrics).

## Set up Superset locally on Linux with Docker Compose

See full instructions [here](https://superset.apache.org/docs/installation/installing-superset-using-docker-compose).

```bash
git clone https://github.com/apache/superset.git
cd superset
```

### Edit `docker/pythonpath_dev/superset_config_docker.py`

* Comment out if exists `# SQLALCHEMY_DATABASE_URI`
* Generate a strong secure key with `openssl rand -base64 42`
* `SECRET_KEY = 'YOUR_OWN_RANDOM_GENERATED_SECRET_KEY'`

### Edit `docker/requirements-local.txt` to add Apache Spark

* `pyhive==0.7.0`

### (Optional) Edit `docker/.env-non-dev` to disable examples
* `SUPERSET_LOAD_EXAMPLES=no`

### Start Superset

```docker-compose -f docker-compose-non-dev.yml up```

### Add a database

* Go to http://localhost:8088/databaseview/list
* Click on + Database

## Import dashboard

### Change the database UUID

* Replace all occurences of the database UUID in the `dashboard_export_XX.zip` file.
   * You can use the `replace_all_in_zip.sh` script.
   * 1st argument is the old UUID: `d6d58c6c-fe82-430c-90b8-4651c77da92f`
   * 2nd argument is the new UUID:
      * Go to http://localhost:8088/databaseview/list
      * Export your database
      * Find the database UUID in the `.yaml` file
  * 3rd argument is the full path to the dashboard `.zip` file
  * The output is copied into the same path with a `.new.zip` file name suffix
* Go to Dashboards and import the `.new.zip` dashboard file
