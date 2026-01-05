import sys
from pyspark.sql import SparkSession
# 1. IMPORT FIX: Added 'col' and 'lit' to avoid NameErrors
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.getOrCreate()

print("=== NYC Taxi Lakehouse Glue Job Started ===")

#  Delta sanity check
spark.sql("SET spark.sql.extensions").show(truncate=False)
spark.sql("SET spark.sql.catalog.spark_catalog").show(truncate=False)


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
from catalog.register_tables import register_all_tables

def main():
    # Glue handles session creation, but we ensure Delta configs are enabled
   

    print("=== NYC Taxi Lakehouse Glue Job Started ===")

    # 1. Bronze – Ingest raw data
    print("Running Bronze ingestion...")
    ingest_bronze(spark, RAW_PATH)

    # 2. Silver – Apply DQ rules and split PASS / FAIL
    print("Running Silver transformations...")
    build_silver(spark)

    # 3. MDM – Build Location Master + stewardship logging
    print("Running MDM Location Master job...")
    build_location_master(spark)

    # 4. Gold – Governance & Quality metrics
    print("Running Gold quality metrics...")
    build_quality_completeness(spark)
    build_quality_accuracy(spark)
    build_quality_timeliness(spark)
    build_quality_consistency(spark)

    print("=== NYC Taxi Lakehouse Core ETL Completed Successfully ===")
   
    # 5. Register tables in Glue Catalog
    register_all_tables(spark)
    print("All tables registered in AWS Glue Data Catalog.")

    # ========================================================
    # 6. FIX: IMPROVED DEBUG BLOCK
    # ========================================================
    print("===== DEBUG: Glue Catalog Visibility & Integrity Check =====")

    db_name = "nyc_taxi_lake"

    # Refresh the catalog metadata to ensure Spark "sees" the tables we just registered
    try:
        spark.catalog.clearCache()
        print(f"Refreshing Catalog metadata for {db_name}...")
    except Exception as e:
        print(f"Metadata refresh warning: {str(e)}")

    # 1️⃣ List databases
    print("Databases visible to Spark:")
    spark.sql("SHOW DATABASES").show(truncate=False)

    # 2️⃣ Verify and List tables
    try:
        print(f"Tables in database {db_name}:")
        tables_df = spark.sql(f"SHOW TABLES IN {db_name}")
        tables_df.show(truncate=False)

        # 3️⃣ Logic Fix: Correctly extracting S3 Path from DESCRIBE output
        table_rows = tables_df.collect()
        for row in table_rows:
            table_name = row["tableName"]
            print(f"\n--- Detailed Metadata for: {db_name}.{table_name} ---")
            
            # Use FORMATTED as it's more reliable for Location data than EXTENDED
            metadata_df = spark.sql(f"DESCRIBE FORMATTED {db_name}.{table_name}")
            
            # Extract Location string
            location_row = metadata_df.filter(col("col_name") == "Location").select("data_type").collect()
            
            if location_row:
                s3_path = location_row[0]["data_type"]
                print(f"✅ Registered S3 Path: {s3_path}")
            else:
                print("⚠️ Location metadata row not found in DESCRIBE output.")

    except Exception as e:
        print(f"❌ Error during Catalog Debug: {str(e)}")

    print("===== END DEBUG =====")

if __name__ == "__main__":
    main()