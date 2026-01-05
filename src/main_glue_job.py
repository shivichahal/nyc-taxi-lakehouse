import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# --------------------------------------------------
# Spark Session (Glue-managed)
# --------------------------------------------------
spark = SparkSession.builder.getOrCreate()

print("=== NYC Taxi Lakehouse Glue Job Started ===")

# --------------------------------------------------
# Iceberg Sanity Check
# --------------------------------------------------
print("=== Iceberg Catalog Sanity Check ===")
spark.sql("SHOW CATALOGS").show(truncate=False)

# --------------------------------------------------
# Imports
# --------------------------------------------------

# Bronze
from bronze.ingest_yellow_taxi import ingest_bronze

# Silver
from silver.silver_transform import build_silver

# MDM
from mdm.location_master_job import build_location_master

# Gold
from gold.quality_metrics import (
    build_quality_completeness,
    build_quality_accuracy,
    build_quality_timeliness,
    build_quality_consistency
)

# Paths
from config.paths import RAW_PATH

# Catalog registration
from catalog.register_tables import register_all_tables


# --------------------------------------------------
# Main
# --------------------------------------------------
def main():

    print("=== NYC Taxi Lakehouse ETL Pipeline Started ===")

    # --------------------------------------------------
    # 1. Bronze – Ingest raw data
    # --------------------------------------------------
    print("Running Bronze ingestion...")
    ingest_bronze(spark, RAW_PATH)

    # --------------------------------------------------
    # 2. Silver – Data Quality & PASS / FAIL split
    # --------------------------------------------------
    print("Running Silver transformations...")
    build_silver(spark)

    # --------------------------------------------------
    # 3. MDM – Location Master & Steward Logs
    # --------------------------------------------------
    print("Running MDM Location Master job...")
    build_location_master(spark)

    # --------------------------------------------------
    # 4. Gold – Quality Metrics
    # --------------------------------------------------
    print("Running Gold quality metrics...")
    build_quality_completeness(spark)
    build_quality_accuracy(spark)
    build_quality_timeliness(spark)
    build_quality_consistency(spark)

    print("=== Core ETL Completed Successfully ===")

    # --------------------------------------------------
    # 5. Register Iceberg tables in Glue Catalog
    # --------------------------------------------------
    print("Registering tables in AWS Glue Catalog...")
    register_all_tables(spark)
    print("All tables registered successfully.")

    # --------------------------------------------------
    # 6. Debug / Validation Block
    # --------------------------------------------------
    print("===== DEBUG: Glue Catalog Visibility & Integrity Check =====")

    db_name = "nyc_taxi_lake"

    try:
        spark.catalog.clearCache()
    except Exception as e:
        print(f"Cache clear warning: {str(e)}")

    # List databases
    print("Databases in Glue Catalog:")
    spark.sql("SHOW DATABASES IN glue_catalog").show(truncate=False)

    # List tables
    print(f"Tables in glue_catalog.{db_name}:")
    tables_df = spark.sql(f"SHOW TABLES IN glue_catalog.{db_name}")
    tables_df.show(truncate=False)

    # Describe tables + show S3 locations
    for row in tables_df.collect():
        table_name = row["tableName"]
        print(f"\n--- Table Metadata: glue_catalog.{db_name}.{table_name} ---")

        metadata_df = spark.sql(
            f"DESCRIBE FORMATTED glue_catalog.{db_name}.{table_name}"
        )

        location_row = (
            metadata_df
            .filter(col("col_name") == "Location")
            .select("data_type")
            .collect()
        )

        if location_row:
            print(f"✅ S3 Location: {location_row[0]['data_type']}")
        else:
            print("⚠️ Location not found in metadata.")

    print("===== END DEBUG =====")
    print("=== NYC Taxi Lakehouse Glue Job Finished ===")


# --------------------------------------------------
# Entry point
# --------------------------------------------------
if __name__ == "__main__":
    main()
