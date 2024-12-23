# tests/test_integration.py
import pytest
from pyspark.sql import SparkSession
from utils.fetch_jobs_util import fetch_jobs
from utils.schema_util import cities_schema, search_terms_schema

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("IntegrationTestJobScraper") \
        .master("local[*]") \
        .getOrCreate()

def test_integration_fetch_jobs(spark):
    # Define search terms
    search_terms = ["Manufacturing Engineer"]
    search_terms_df = spark.createDataFrame([(term,) for term in search_terms], schema=search_terms_schema)

    # Call API for Cincinnati
    cities_data_a = [("Louisville", "KY")]
    cities_df_a = spark.createDataFrame(cities_data_a, schema=cities_schema)
    result_a_df = fetch_jobs(spark, cities_df_a, search_terms_df)
    result_a_filtered_df = result_a_df.filter(result_a_df.location.contains("Louisville"))
    result_a_count = result_a_filtered_df.count()

    # Call API for multiple cities and filter to Cincinnati
    cities_data_b = [("Louisville", "KY"), ("Covington", "KY"), ("Cincinnati", "OH"), ("Mayfield", "KY")]
    cities_df_b = spark.createDataFrame(cities_data_b, schema=cities_schema)
    result_b_df = fetch_jobs(spark, cities_df_b, search_terms_df)
    result_b_filtered_df = result_b_df.filter(result_b_df.location.contains("Louisville"))
    result_b_count = result_b_filtered_df.count()

    # Test: Count Result A <= Count Result B + d, where d < 20
    d = 20
    assert result_a_count <= result_b_count + d, f"Result A count {result_a_count} is not <= Result B count {result_b_count} + {d}"