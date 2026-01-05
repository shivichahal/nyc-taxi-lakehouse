from pyspark.sql.functions import *
from config.paths import SILVER_PASS_PATH, MDM_PATH
from governance.audit_logger import log_steward_activity


def build_location_master(spark):
    """
    Builds Location Master (MDM) from Silver PASS data.
    Logs system stewardship actions for governance.
    """

    #  Read validated Silver data
    location_df = (
        spark.read.format("delta").load(SILVER_PASS_PATH)
        .select(col("PULocationID").alias("location_id"))
        .distinct()
        .withColumn("confidence_score", lit(0.95))
        .withColumn("lifecycle_state", lit("ACTIVE"))
        .withColumn("effective_start_ts", current_timestamp())
    )

    #  Write Location Master (Delta table)
    location_df.write.format("delta") \
        .mode("overwrite") \
        .save(MDM_PATH)

    #  Log stewardship activity (SYSTEM-generated)
    for row in location_df.select("location_id").collect():
        log_steward_activity(
            spark=spark,
            entity_name="location_master",
            record_id=str(row["location_id"]),
            action="AUTO_CREATE",
            performed_by="SYSTEM",
            comments="Auto-created from Silver PASS taxi data"
        )
