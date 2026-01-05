from config.paths import (
    BRONZE_PATH,
    SILVER_PASS_PATH,
    SILVER_FAIL_PATH,
    MDM_PATH,
    GOLD_PATH,
    STEWARD_LOG_PATH
)

DATABASE = "nyc_taxi_lake"


def _validate_path(name, path):
    if not path or path.strip() == "":
        raise ValueError(f"{name} is EMPTY or INVALID: '{path}'")
    print(f"[OK] {name} = {path}")

def register_all_tables(spark):
    """
    Registers all Delta tables in AWS Glue Data Catalog
    using centralized path configuration.
    Safe to run multiple times.
    """
    _validate_path("BRONZE_PATH", BRONZE_PATH)
    _validate_path("SILVER_PASS_PATH", SILVER_PASS_PATH)
    _validate_path("SILVER_FAIL_PATH", SILVER_FAIL_PATH)
    _validate_path("MDM_PATH", MDM_PATH)
    _validate_path("GOLD_PATH", GOLD_PATH)
    _validate_path("STEWARD_LOG_PATH", STEWARD_LOG_PATH)
    # --------------------------------------------------
    # Create Database
    # --------------------------------------------------
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {DATABASE}
    """)

    # --------------------------------------------------
    # Bronze
    # --------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.bronze_yellow_taxi
        USING DELTA
        LOCATION '{BRONZE_PATH}'
    """)

    # --------------------------------------------------
    # Silver
    # --------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.silver_yellow_taxi_pass
        USING DELTA
        LOCATION '{SILVER_PASS_PATH}'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.silver_yellow_taxi_fail
        USING DELTA
        LOCATION '{SILVER_FAIL_PATH}'
    """)

    # --------------------------------------------------
    #MDM
    # --------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.location_master
        USING DELTA
        LOCATION '{MDM_PATH}'
    """)

    # --------------------------------------------------
    # Gold – Quality Metrics
    # --------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_completeness
        USING DELTA
        LOCATION '{GOLD_PATH}/completeness'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_accuracy
        USING DELTA
        LOCATION '{GOLD_PATH}/accuracy'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_timeliness
        USING DELTA
        LOCATION '{GOLD_PATH}/timeliness'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_consistency
        USING DELTA
        LOCATION '{GOLD_PATH}/consistency'
    """)

    # --------------------------------------------------
    # Governance / Steward Logs
    # --------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.steward_activity_log
        USING DELTA
        LOCATION '{STEWARD_LOG_PATH}'
    """)
