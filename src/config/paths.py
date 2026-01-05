RAW_PATH = "s3://nyc-taxi-lakehouse-bucket/raw/yellow_tripdata_2025-08.parquet"
BRONZE_PATH = "s3://nyc-taxi-lakehouse-bucket/bronze/yellow_taxi"
SILVER_PATH = "s3://nyc-taxi-lakehouse-bucket/silver/yellow_taxi"
MDM_PATH = "s3://nyc-taxi-lakehouse-bucket/mdm/location_master"
GOLD_PATH = "s3://nyc-taxi-lakehouse-bucket/gold/quality_metrics"
STEWARD_LOG_PATH = (
    "s3://nyc-taxi-lakehouse-bucket/governance/steward_activity"
)
