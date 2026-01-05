from pyspark.sql.functions import *
from config.paths import BRONZE_PATH

def ingest_bronze(spark, raw_path):
    df = spark.read.parquet(raw_path)
    df.withColumn("ingestion_ts", current_timestamp()) \
      .write.format("delta").mode("append").save(BRONZE_PATH)
