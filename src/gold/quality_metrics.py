from pyspark.sql.functions import *
from config.paths import SILVER_PATH, GOLD_PATH

def build_quality_metrics(spark):
    spark.read.format("delta").load(SILVER_PATH) \
        .groupBy(to_date("tpep_pickup_datetime").alias("date")) \
        .count() \
        .write.format("delta").mode("overwrite").save(GOLD_PATH)
