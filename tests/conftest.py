# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("TestJobScraper") \
        .master("local[*]") \
        .getOrCreate()