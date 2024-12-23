from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
from utils.schema_util import JOB_SCHEMA
from utils.normalization_util import normalize_spark_dataframe
from utils.fetch_jobs_util import fetch_jobs
import logging

logger = logging.getLogger(__name__)

def fetch_jobs_transform(
    spark: SparkSession, 
    cities: DataFrame,
    search_terms: DataFrame,
    output: DataFrame = None,
    hours_old: int = 7200,
    results_wanted: int = 5000,
    max_workers: int = 10
) -> DataFrame:
    """
    Fetches job postings for specified cities and search terms using jobspy
    and returns the results as a normalized Spark DataFrame.

    Args:
        spark (SparkSession): Spark session.
        cities (DataFrame): DataFrame containing city and state information.
        search_terms (DataFrame): DataFrame containing search terms.
        output (DataFrame, optional): Output DataFrame for results.
        hours_old (int, optional): Time filter for job postings in hours. Defaults to 7200.
        results_wanted (int, optional): Number of results to fetch per combination. Defaults to 5000.
        max_workers (int, optional): Maximum number of concurrent workers. Defaults to 10.

    Returns:
        DataFrame: Normalized Spark DataFrame with job postings.
    """
    logger.info("Starting job fetching process...")
    
    # Use the fetch_jobs utility to get job postings
    jobs_df = fetch_jobs(
        spark=spark,
        cities=cities,
        search_terms=search_terms,
        results_wanted=results_wanted,
        hours_old=hours_old,
        max_workers=max_workers
    )
    
    # Add metadata column with timestamp
    jobs_df = jobs_df.withColumn("fetch_timestamp", lit(datetime.now().isoformat()))
    
    # Log results
    job_count = jobs_df.count()
    logger.info(f"Fetched {job_count} job postings.")
    
    return jobs_df
