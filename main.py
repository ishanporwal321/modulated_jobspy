from utils.logging_util import configure_logging
from utils.spark_util import create_spark_session
from utils.schema_util import cities_schema, search_terms_schema
from utils.fetch_jobs_util import fetch_jobs

import os
from datetime import datetime

logger = configure_logging()

# Initialize Spark
spark = create_spark_session("JobScraper")

# Input data
cities_data = [("Cincinnati", "OH")]

search_terms = [
    "Manufacturing Engineer",
    "Industrial Designer",
    "Process Engineer",
    "Mechanical Engineer",
    "Quality Assurance Specialist",
    "Production Manager",
    "Automation Technician",
    "CNC Machinist",
    "Supply Chain Analyst",
    "Lean Manufacturing Consultant"
]

cities_df = spark.createDataFrame(cities_data, schema=cities_schema)
search_terms_df = spark.createDataFrame([(term,) for term in search_terms], schema=search_terms_schema)

output_dir = "job_results"
os.makedirs(output_dir, exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

try:
    results_df = fetch_jobs(spark, cities_df, search_terms_df)
    csv_path = f"{output_dir}/jobs_{timestamp}.csv"
    results_df.toPandas().to_csv(csv_path, index=False)
    logger.info(f"Results saved to: {csv_path}")
finally:
    spark.stop()
