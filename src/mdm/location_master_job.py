from pyspark.sql.functions import *
from delta.tables import DeltaTable

from config.paths import SILVER_PATH, MDM_PATH
from governance.audit_logger import log_steward_activity


def build_location_master(spark):
    """
    Builds Location Master from Silver data
    and logs stewardship activity for governance
    """

    # Read Silver (validated trips)
    locations_df = (
        spark.read.format("delta").load(SILVER_PATH)
        .select(col("PULocationID").alias("location_id"))
        .distinct()
        .withColumn("confidence_score", lit(0.95))
        .withColumn("lifecycle_state", lit("ACTIVE"))
        .withColumn("effective_start_ts", current_timestamp())
    )

    # Write / overwrite Location Master (simple version)
    locations_df.write.format("delta") \
        .mode("overwrite") \
        .save(MDM_PATH)

    # Log steward activity (SYSTEM action)
    # This records that the system auto-created master data
    for row in locations_df.select("location_id").collect():
        log_steward_activity(
            spark=spark,
            entity_name="location_master",
            record_id=str(row["location_id"]),
            action="AUTO_CREATE",
            performed_by="SYSTEM",
            comments="Auto-created from Silver taxi trips"
        )
