from pyspark import pipelines as dp


@dp.materialized_view(name="raw_sales")
def raw_sales():
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/sales.csv")
    )