def register_all_tables(spark):
    """
    Registers all Delta Lake tables in AWS Glue Data Catalog.
    Safe to run multiple times.
    """

    # --------------------------------------------------
    # Create Database
    # --------------------------------------------------
    spark.sql("""
        CREATE DATABASE IF NOT EXISTS nyc_taxi
    """)

    # --------------------------------------------------
    #  Bronze Tables
    # --------------------------------------------------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nyc_taxi.bronze_yellow_taxi
        USING DELTA
        LOCATION 's3://nyc-taxi-lakehouse-shivani-bucket/src/bronze/yellow_taxi'
    """)

    # --------------------------------------------------
    #  Silver Tables
    # --------------------------------------------------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nyc_taxi.silver_yellow_taxi_pass
        USING DELTA
        LOCATION 's3://nyc-taxi-lakehouse-shivani-bucket/src/silver/pass/yellow_taxi'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nyc_taxi.silver_yellow_taxi_fail
        USING DELTA
        LOCATION 's3://nyc-taxi-lakehouse-shivani-bucket/src/silver/fail/yellow_taxi'
    """)

    # --------------------------------------------------
    #  MDM Tables
    # --------------------------------------------------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nyc_taxi.location_master
        USING DELTA
        LOCATION 's3://nyc-taxi-lakehouse-shivani-bucket/src/mdm/location_master'
    """)

    # --------------------------------------------------
    #  Gold – Quality Metrics
    # --------------------------------------------------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nyc_taxi.quality_completeness
        USING DELTA
        LOCATION 's3://nyc-taxi-lakehouse-shivani-bucket/src/gold/quality_metrics/completeness'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nyc_taxi.quality_accuracy
        USING DELTA
        LOCATION 's3://nyc-taxi-lakehouse-shivani-bucket/src/gold/quality_metrics/accuracy'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nyc_taxi.quality_timeliness
        USING DELTA
        LOCATION 's3://nyc-taxi-lakehouse-shivani-bucket/src/gold/quality_metrics/timeliness'
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nyc_taxi.quality_consistency
        USING DELTA
        LOCATION 's3://nyc-taxi-lakehouse-shivani-bucket/src/gold/quality_metrics/consistency'
    """)

    # --------------------------------------------------
    #Governance / Steward Logs
    # --------------------------------------------------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nyc_taxi.steward_activity_log
        USING DELTA
        LOCATION 's3://nyc-taxi-lakehouse-shivani-bucket/src/governance/steward_activity'
    """)
