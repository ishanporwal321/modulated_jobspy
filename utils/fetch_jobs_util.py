import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import lit
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from jobspy import scrape_jobs
from utils.normalization_util import normalize_spark_dataframe
import random
import time
import requests

logger = logging.getLogger(__name__)

# Updated schema
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

def fetch_jobs(spark, cities_df: DataFrame, search_terms_df: DataFrame, results_wanted=5000, hours_old=7200, max_workers=10) -> DataFrame:
    """
    Fetch job listings based on cities and search terms.
    """
    city_list = [f"{row.city}, {row.state}" for row in cities_df.collect()]
    search_term_list = [row.term for row in search_terms_df.collect()]
    
    logger.info(f"Fetching jobs for {len(city_list)} cities and {len(search_term_list)} search terms.")

    def fetch_for_city_and_term(city, term):
        try:
            # Zyte Smart Proxy Manager settings
            api_key = "9e4436e8807c4d89b698b1e2272ada85"  # Replace with your actual Zyte API key

            proxies = {
                scheme: f"http://{api_key}:@api.zyte.com:8011" for scheme in ("http", "https")
            }

            # Rotate user-agent strings
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
                # Add more user-agent strings as needed
            ]
            headers = {
                "User-Agent": random.choice(user_agents),
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.linkedin.com",
            }
            logger.info(f"Fetching jobs for {city} - {term} using proxies: {proxies} and headers: {headers}")

            # Scrape jobs
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "zip_recruiter", "glassdoor", "google"],
                search_term=term,
                location=city,
                results_wanted=results_wanted,
                hours_old=hours_old,
                country_indeed='USA',
                proxies=proxies,  # Pass the proxies to the request
                headers=headers  # Pass the headers to the request
            )
            logger.info(f"Fetched {len(jobs)} jobs for {city} - {term} using proxy: {proxies}")
            time.sleep(random.uniform(1, 3))

            return pd.DataFrame(jobs)
        except Exception as e:
            logger.error(f"Error fetching jobs for {city} - {term}: {str(e)}")
            return pd.DataFrame()

    all_jobs = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_for_city_and_term, city, term): (city, term)
            for city in city_list
            for term in search_term_list
        }
        for future in as_completed(futures):
            result = future.result()
            if not result.empty:
                all_jobs.append(result)

    if not all_jobs:
        logger.warning("No jobs found.")
        return spark.createDataFrame([], schema=JOB_SCHEMA)

    combined_df = pd.concat(all_jobs, ignore_index=True)
    logger.info(f"Total jobs fetched: {len(combined_df)}")

    # Map job data to the defined schema
    column_mapping = {
        'title': 'title',
        'company': 'company',
        'location': 'location',
        'description': 'description',
        'job_url': 'job_url',
        'date_posted': 'date_posted',
        'site_name': 'site',
        'salary_min': 'min_amount',
        'salary_max': 'max_amount',
        'salary_period': 'interval'
    }
    
    selected_columns = {}
    for target_col, source_col in column_mapping.items():
        if source_col in combined_df.columns:
            selected_columns[target_col] = combined_df[source_col]
        else:
            selected_columns[target_col] = None  # Fill missing columns with None

    # Create final DataFrame
    final_df = pd.DataFrame(selected_columns).drop_duplicates(subset=["job_url"])

    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(final_df, schema=JOB_SCHEMA)
    
    # Normalize data
    spark_df = normalize_spark_dataframe(spark_df)

    # Add fetch timestamp
    spark_df = spark_df.withColumn("fetch_timestamp", lit(datetime.now().isoformat()))
    logger.info(f"Successfully processed {spark_df.count()} jobs.")
    return spark_df