# tests/test_spark_util.py
from utils.spark_util import create_spark_session
from pyspark.sql import SparkSession

def test_create_spark_session():
    # Stop any existing Spark session
    if SparkSession.getActiveSession() is not None:
        SparkSession.getActiveSession().stop()
    
    # Create a new Spark session
    spark = create_spark_session("TestApp")
    assert spark is not None
    assert spark.sparkContext.appName == "TestApp"
    spark.stop()