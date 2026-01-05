from pyspark.sql import SparkSession
from config.paths import RAW_PATH
from bronze.ingest_yellow_taxi import ingest_bronze
from silver.silver_transform import build_silver
from mdm.location_master_job import build_location_master
from gold.quality_metrics import build_quality_metrics

spark = SparkSession.builder.getOrCreate()

ingest_bronze(spark, RAW_PATH)
build_silver(spark)
build_location_master(spark)
build_quality_metrics(spark)
