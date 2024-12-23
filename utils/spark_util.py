from pyspark.sql import SparkSession

def create_spark_session(app_name="SparkApp"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
