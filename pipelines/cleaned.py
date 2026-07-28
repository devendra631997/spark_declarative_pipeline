from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date


@dp.materialized_view(name="cleaned_sales")
def cleaned_sales():
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/sales.csv")
        .dropDuplicates()
        .dropna()
        .withColumn("date", to_date(col("date")))
        .withColumn(
            "total_amount",
            col("quantity") * col("price")
        )
    )