from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Define input schema for cities
cities_schema = StructType([
    StructField("city", StringType(), False),
    StructField("state", StringType(), False)
])

search_terms_schema = StructType([
    StructField("term", StringType(), False)
])

JOB_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("company", StringType(), True),
    StructField("location", StringType(), True),
    StructField("description", StringType(), True),
    StructField("job_url", StringType(), True),
    StructField("date_posted", StringType(), True),
    StructField("site_name", StringType(), True),
    StructField("salary_min", FloatType(), True),
    StructField("salary_max", FloatType(), True),
    StructField("salary_period", StringType(), True)
])
