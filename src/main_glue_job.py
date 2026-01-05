from pyspark.sql import SparkSession

# Bronze
from bronze.ingest_yellow_taxi import ingest_bronze

# Silver
from silver.silver_transform import build_silver

# MDM
from mdm.location_master_job import build_location_master

# Gold – all quality dimensions from ONE file
from gold.quality_metrics import (
    build_quality_completeness,
    build_quality_accuracy,
    build_quality_timeliness,
    build_quality_consistency
)

# Paths
from config.paths import RAW_PATH


def main():
    # Create Spark session (Glue provides this automatically)
    spark = SparkSession.builder.getOrCreate()

    print("=== NYC Taxi Lakehouse Glue Job Started ===")

    #  Bronze – ingest raw data
    print("Running Bronze ingestion...")
    ingest_bronze(spark, RAW_PATH)

    # Silver – apply DQ rules and split PASS / FAIL
    print("Running Silver transformations...")
    build_silver(spark)

    # MDM – build Location Master + stewardship logging
    print("Running MDM Location Master job...")
    build_location_master(spark)

    #  Gold – governance & quality metrics
    print("Running Gold quality metrics...")
    build_quality_completeness(spark)
    build_quality_accuracy(spark)
    build_quality_timeliness(spark)
    build_quality_consistency(spark)

    print("=== NYC Taxi Lakehouse Glue Job Completed Successfully ===")


if __name__ == "__main__":
    main()
