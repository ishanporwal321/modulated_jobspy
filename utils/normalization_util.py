from pyspark.sql import DataFrame
from pyspark.sql.functions import trim, regexp_replace, col
from pyspark.sql.types import FloatType

def normalize_spark_dataframe(df: DataFrame) -> DataFrame:
    text_columns = ["title", "company", "description", "location"]
    for col_name in text_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                trim(regexp_replace(col(col_name), r'\s+', ' '))
            )
    
    if "date_posted" in df.columns:
        df = df.withColumn(
            "date_posted",
            trim(col("date_posted"))
        )
    
    for col_name in ["salary_min", "salary_max"]:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                col(col_name).cast(FloatType())
            )
    
    return df
