from config.paths import (
    BRONZE_PATH,
    SILVER_PASS_PATH,
    SILVER_FAIL_PATH,
    MDM_PATH,
    GOLD_PATH,
    STEWARD_LOG_PATH
)

DATABASE = "nyc_taxi_lake"


def _validate_path(name: str, path: str):
    """
    Fail fast if any path is empty or invalid.
    Prevents Spark 'IllegalArgumentException: empty string'.
    """
    if path is None:
        raise ValueError(f"{name} is None (check paths.py packaging)")
    if not isinstance(path, str):
        raise ValueError(f"{name} must be a string, got {type(path)}")
    if not path.strip():
        raise ValueError(f"{name} is EMPTY or INVALID: '{path}'")

    print(f"[OK] {name} = {path.strip()}")


def register_all_tables(spark):
    """
    Registers ALL Iceberg tables in AWS Glue Data Catalog.
    Safe to run multiple times.
    """

    print("===== REGISTER ALL TABLES STARTED =====")

    # --------------------------------------------------
    # 1. Validate all paths
    # --------------------------------------------------
    _validate_path("BRONZE_PATH", BRONZE_PATH)
    _validate_path("SILVER_PASS_PATH", SILVER_PASS_PATH)
    _validate_path("SILVER_FAIL_PATH", SILVER_FAIL_PATH)
    _validate_path("MDM_PATH", MDM_PATH)
    _validate_path("GOLD_PATH", GOLD_PATH)
    _validate_path("STEWARD_LOG_PATH", STEWARD_LOG_PATH)

    # --------------------------------------------------
    # 2. Create Glue database
    # --------------------------------------------------
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {DATABASE}
    """)
    print("[OK] Database created or already exists")

    # --------------------------------------------------
    # 3. Bronze table
    # --------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.bronze_yellow_taxi
        USING ICEBERG
        LOCATION '{BRONZE_PATH}'
    """)
    print("[OK] Bronze table registered")

    # --------------------------------------------------
    # 4. Silver tables
    # --------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.silver_yellow_taxi_pass
        USING ICEBERG
        LOCATION '{SILVER_PASS_PATH}'
    """)
    print("[OK] Silver PASS table registered")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.silver_yellow_taxi_fail
        USING ICEBERG
        LOCATION '{SILVER_FAIL_PATH}'
    """)
    print("[OK] Silver FAIL table registered")

    # --------------------------------------------------
    # 5. MDM table
    # --------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.location_master
        USING ICEBERG
        LOCATION '{MDM_PATH}'
    """)
    print("[OK] MDM table registered")

    # --------------------------------------------------
    # 6. Gold – Quality Metrics tables
    # --------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_completeness
        USING ICEBERG
        LOCATION '{GOLD_PATH}/completeness'
    """)
    print("[OK] Gold Completeness table registered")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_accuracy
        USING ICEBERG
        LOCATION '{GOLD_PATH}/accuracy'
    """)
    print("[OK] Gold Accuracy table registered")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_timeliness
        USING ICEBERG
        LOCATION '{GOLD_PATH}/timeliness'
    """)
    print("[OK] Gold Timeliness table registered")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.quality_consistency
        USING ICEBERG
        LOCATION '{GOLD_PATH}/consistency'
    """)
    print("[OK] Gold Consistency table registered")

    # --------------------------------------------------
    # 7. Steward logs table
    # --------------------------------------------------
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.steward_activity_log
        USING ICEBERG
        LOCATION '{STEWARD_LOG_PATH}'
    """)
    print("[OK] Steward Activity Log table registered")

    print("===== ALL TABLES REGISTERED SUCCESSFULLY =====")
