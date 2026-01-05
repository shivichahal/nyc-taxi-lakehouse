from pyspark.sql.functions import *
from config.paths import BRONZE_PATH
from config.paths import RAW_PATH

def ingest_bronze(spark, RAW_PATH):
    df = spark.read.parquet(RAW_PATH)
    df.withColumn("ingestion_ts", current_timestamp()) \
      .write.format("delta").mode("append").save(BRONZE_PATH)
