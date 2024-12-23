# test_normalization_util.py
from pyspark.sql import SparkSession
from utils.normalization_util import normalize_spark_dataframe

def test_normalize_spark_dataframe(spark):
    data = [("  Title  ", "  Company  ", "  Location  ", "  Description  ", 1000.0, 2000.0)]
    columns = ["title", "company", "location", "description", "salary_min", "salary_max"]
    df = spark.createDataFrame(data, columns)
    
    normalized_df = normalize_spark_dataframe(df)
    row = normalized_df.collect()[0]
    
    assert row.title == "Title"
    assert row.company == "Company"
    assert row.location == "Location"
    assert row.description == "Description"
    assert row.salary_min == 1000.0
    assert row.salary_max == 2000.0