import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, lit
from config.paths import RAW_PATH
from awsglue.context import GlueContext
from pyspark.context import SparkContext


# ======================================================
# Read Glue Job Arguments
# ======================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])

S3_BUCKET = args["S3_BUCKET"]

print(f"Starting Glue Job: {args['JOB_NAME']}")
print(f"Using S3 Bucket: {S3_BUCKET}")

# ======================================================
# Spark Session with Delta Lake Enabled
# ======================================================
spark = (
    SparkSession.builder
    .appName("nyc-taxi-lakehouse")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .enableHiveSupport()
    .getOrCreate()
)



print("Spark session initialized with Delta Lake support")

# ======================================================
# Import Jobs
# ======================================================
from bronze.ingest_yellow_taxi import ingest_bronze
from silver.silver_transform import build_silver
from mdm.location_master_job import build_location_master
from gold.quality_metrics import (
    build_quality_completeness,
    build_quality_accuracy,
    build_quality_timeliness,
    build_quality_consistency
)
from catalog.register_tables import register_all_tables

# ======================================================
# Execute Pipeline
# ======================================================
def main():

    
    print("===== BRONZE LAYER STARTED =====")
    ingest_bronze(spark)

    
    print("===== SILVER LAYER STARTED =====")
    build_silver(spark)

    print("===== MDM LAYER STARTED =====")
    build_location_master(spark)

    print("===== GOLD LAYER STARTED =====")
    build_quality_completeness(spark)
    build_quality_accuracy(spark)
    build_quality_timeliness(spark)
    build_quality_consistency(spark)

    print("===== REGISTERING TABLES IN GLUE CATALOG =====")
    register_all_tables(spark)

    print("===== NYC TAXI LAKEHOUSE JOB COMPLETED SUCCESSFULLY =====")


if __name__ == "__main__":
    main()
