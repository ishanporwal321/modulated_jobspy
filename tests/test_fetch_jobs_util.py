import pytest
from pyspark.sql import SparkSession
from utils.fetch_jobs_util import fetch_jobs
from utils.schema_util import cities_schema, search_terms_schema

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestJobScraper") \
        .master("local[*]") \
        .getOrCreate()

def test_fetch_jobs(spark):
    cities_data = [("Louisville", "KY")]
    search_terms = ["Manufacturing Engineer"]
    cities_df = spark.createDataFrame(cities_data, schema=cities_schema)
    search_terms_df = spark.createDataFrame([(term,) for term in search_terms], schema=search_terms_schema)

    results_df = fetch_jobs(spark, cities_df, search_terms_df)
    assert results_df is not None
    assert results_df.count() >= 0  # Adjust based on expected results

def test_fetch_jobs_with_missing_fields(spark):
    # Mock data with various missing fields
    cities_data = [("Louisville", "KY")]
    search_terms = ["Manufacturing Engineer"]
    cities_df = spark.createDataFrame(cities_data, schema=cities_schema)
    search_terms_df = spark.createDataFrame([(term,) for term in search_terms], schema=search_terms_schema)

    # Mock the fetch_jobs function to return jobs with missing fields
    def mock_scrape_jobs(*args, **kwargs):
        return [
            {"title": "Engineer", "company": "Tech Corp", "location": "Louisville, KY", "job_url": "http://example.com/job1"},
            {"title": "Designer", "company": "Design Inc", "location": "Louisville, KY", "job_url": "http://example.com/job2", "description": "Design stuff"},
            {"title": "Manager", "company": "Manage LLC", "location": "Louisville, KY", "job_url": "http://example.com/job3", "salary_min": 50000},
            {"title": "Analyst", "company": "Analyze Co", "location": "Louisville, KY", "job_url": "http://example.com/job4", "salary_max": 70000},
            {"title": "Consultant", "company": "Consulting Group", "location": "Louisville, KY", "job_url": "http://example.com/job5", "salary_period": "yearly"}
        ]

    # Patch the scrape_jobs function
    original_scrape_jobs = fetch_jobs.__globals__['scrape_jobs']
    fetch_jobs.__globals__['scrape_jobs'] = mock_scrape_jobs

    try:
        results_df = fetch_jobs(spark, cities_df, search_terms_df)
        assert results_df is not None
        assert results_df.count() == 5  # Ensure all jobs are included

        # Check that missing fields are handled correctly
        rows = results_df.collect()
        for row in rows:
            assert row.description is None or isinstance(row.description, str)
            assert row.date_posted is None or isinstance(row.date_posted, str)
            assert row.salary_min is None or isinstance(row.salary_min, float)
            assert row.salary_max is None or isinstance(row.salary_max, float)
            assert row.salary_period is None or isinstance(row.salary_period, str)
    finally:
        # Restore the original function
        fetch_jobs.__globals__['scrape_jobs'] = original_scrape_jobs