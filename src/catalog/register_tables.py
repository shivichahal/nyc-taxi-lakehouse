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
    Registers all Iceberg tables in AWS Glue Data Catalog.
    Safe to run multiple times.
    """

    # -----------------------
    # Validate paths
    # -----------------------
    _validate_path("BRONZE_PATH", BRONZE_PATH)
    _validate_path("SILVER_PASS_PATH", SILVER_PASS_PATH)
    _validate_path("SILVER_FAIL_PATH", SILVER_FAIL_PATH)
    _validate_path("MDM_PATH", MDM_PATH)
    _validate_path("GOLD_PATH", GOLD_PATH)
    _validate_path("STEWARD_LOG_PATH", STEWARD_LOG_PATH)

    # -----------------------
    # Create database
    # -----------------------
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {DATABASE}
    """)
    print("[OK]1 Database")

    # -----------------------
    # Bronze
    # -----------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.bronze_yellow_taxi
        USING ICEBERG
        LOCATION '{BRONZE_PATH}'
    """)
    print("[OK]2 Bronze")

    # -----------------------
    # Silver
    # -----------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.silver_yellow_taxi_pass
        USING ICEBERG
        LOCATION '{SILVER_PASS_PATH}'
    """)
    print("[OK]3 Silver PASS")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.silver_yellow_taxi_fail
        USING ICEBERG
        LOCATION '{SILVER_FAIL_PATH}'
    """)
    print("[OK]4 Silver FAIL")

    # -----------------------
    # MDM
    # -----------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.location_master
        USING ICEBERG
        LOCATION '{MDM_PATH}'
    """)
    print("[OK]5 MDM")

    # -----------------------
    # Gold – Quality Metrics
    # -----------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_completeness
        USING ICEBERG
        LOCATION '{GOLD_PATH}/completeness'
    """)
    print("[OK]6 Gold Completeness")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_accuracy
        USING ICEBERG
        LOCATION '{GOLD_PATH}/accuracy'
    """)
    print("[OK]7 Gold Accuracy")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_timeliness
        USING ICEBERG
        LOCATION '{GOLD_PATH}/timeliness'
    """)
    print("[OK]8 Gold Timeliness")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_consistency
        USING ICEBERG
        LOCATION '{GOLD_PATH}/consistency'
    """)
    print("[OK]9 Gold Consistency")

    # -----------------------
    # Steward Logs
    # -----------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.steward_activity_log
        USING ICEBERG
        LOCATION '{STEWARD_LOG_PATH}'
    """)
    print("[OK]10 Steward Logs")
