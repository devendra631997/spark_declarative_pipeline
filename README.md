# Spark Declarative Pipeline

A local implementation of a **Spark Declarative Pipeline** using PySpark Pipelines APIs.

This project defines a declarative data processing graph where datasets are represented as pipeline entities and Spark manages dependency resolution, execution ordering, and materialization.

## Architecture

The pipeline consists of two materialized views:

```
                    sales.csv
                       |
                       v
                +--------------+
                |  raw_sales   |
                | materialized |
                |    view      |
                +--------------+
                       |
                       v
                +--------------+
                | cleaned_sales|
                | materialized |
                |    view      |
                +--------------+
```

## Project Structure

```
sdp/
│
├── data/
│   └── sales.csv
│
├── pipelines/
│   ├── __init__.py
│   ├── raw.py
│   └── cleaned.py
│
├── pipeline.yml
│
└── README.md
```

## Pipeline Specification

`pipeline.yml`

```yaml
name: sales_pipeline

libraries:
  - glob:
      include: pipelines/**
```

The pipeline specification defines:

* Pipeline name.
* Python source discovery locations.
* Dataset definitions loaded into the pipeline graph.

## Dataset Definitions

### Raw Dataset

`pipelines/raw.py`

```python
from pyspark import pipelines as dp


@dp.materialized_view(name="raw_sales")
def raw_sales():
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/sales.csv")
    )
```

`raw_sales` is declared as a materialized view.

The function returns a Spark DataFrame. The pipeline engine registers this function as a dataset-producing flow.

## Cleaned Dataset

`pipelines/cleaned.py`

```python
from pyspark import pipelines as dp


@dp.materialized_view(name="cleaned_sales")
def cleaned_sales():
    return spark.sql(
        "SELECT * FROM raw_sales"
    )
```

`cleaned_sales` declares a dependency on `raw_sales`.

Spark Declarative Pipeline builds the dependency graph and executes upstream datasets before downstream datasets.

## Execution Model

The pipeline lifecycle:

```
Load pipeline specification
          |
          v
Discover pipeline definitions
          |
          v
Register datasets and flows
          |
          v
Create dataflow graph
          |
          v
Resolve dependencies
          |
          v
Execute materialized views
```

## Running the Pipeline

### Pipeline Validation

Run:

```bash
spark-pipelines dry-run --spec pipeline.yml
```

Example:

```
Creating dataflow graph...
Registering graph elements...
Starting run...
Run is COMPLETED.
```

Dry run validates:

* Pipeline specification.
* Dataset registration.
* Dependency graph construction.
* Dataset resolution.

## Pipeline Execution

Run:

```bash
spark-pipelines run --spec pipeline.yml
```

Example execution:

```
Flow spark_catalog.default.raw_sales is RUNNING.
Flow spark_catalog.default.raw_sales has COMPLETED.

Flow spark_catalog.default.cleaned_sales is RUNNING.
Flow spark_catalog.default.cleaned_sales has COMPLETED.

Run is COMPLETED.
```

## Storage Behaviour

Local Spark Declarative Pipeline execution creates managed Spark datasets.

Default location:

```
spark-warehouse/
├── raw_sales/
└── cleaned_sales/
```

Datasets are registered in:

```
spark_catalog.default
```

Example:

```
spark_catalog.default.raw_sales
spark_catalog.default.cleaned_sales
```

The physical storage location is controlled by Spark SQL warehouse configuration:

```
spark.sql.warehouse.dir
```

## Dependency Resolution

Dataset dependencies are resolved through dataset references.

Example:

```python
spark.sql(
    "SELECT * FROM raw_sales"
)
```

creates a dependency relationship:

```
raw_sales
    |
    v
cleaned_sales
```

The pipeline engine uses this graph to determine execution order.

## Supported Dataset Types

Spark Pipelines supports dataset declarations such as:

* Materialized views
* Tables
* Streaming tables
* Append flows

Example:

```python
@dp.materialized_view(name="dataset_name")
def dataset_name():
    return dataframe
```

## Troubleshooting

### TABLE_OR_VIEW_NOT_FOUND

Example:

```
[TABLE_OR_VIEW_NOT_FOUND] raw_sales cannot be found
```

Possible causes:

* Dataset name mismatch.
* Upstream dataset was not registered.
* Pipeline file was not discovered.
* Dependency reference does not match the declared dataset name.

### Pipeline Specification Validation Error

Example:

```
PIPELINE_SPEC_UNEXPECTED_FIELD
```

Cause:

The field is not supported by the installed Spark Pipelines CLI version.

Validate the pipeline specification against the installed package version.

## Environment

Example environment:

```
Python 3.10
PySpark
Spark Declarative Pipelines API
Java 17+
```

## Future Extensions

* Add data quality expectations.
* Add incremental processing.
* Add streaming datasets.
* Add schema enforcement.
* Add CI/CD execution.
* Deploy pipeline definitions to managed Spark environments.
