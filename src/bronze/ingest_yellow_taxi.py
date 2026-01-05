from pyspark.sql.functions import current_timestamp
from config.paths import BRONZE_PATH, RAW_PATH

def ingest_bronze(spark):
    df = spark.read.parquet(RAW_PATH)

    if df.rdd.isEmpty():
        print("No data found in RAW_PATH. Skipping Bronze write.")
        return

    (
        df.withColumn("ingestion_ts", current_timestamp())
          .write
          .format("delta")
          .mode("append")
          .save(BRONZE_PATH)
    )

    print("Bronze Delta write completed successfully.")
