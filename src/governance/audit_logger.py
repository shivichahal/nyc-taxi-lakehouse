from pyspark.sql.functions import *
from config.paths import STEWARD_LOG_PATH

def log_steward_activity(
    spark,
    entity_name,
    record_id,
    action,
    performed_by,
    comments=None
):
    """
    entity_name   : e.g. 'location_master'
    record_id     : business key or master id
    action        : APPROVE, REJECT, AUTO_MERGE, FIXED
    performed_by  : user id or SYSTEM
    comments      : optional notes
    """

    df = spark.createDataFrame(
        [(entity_name, record_id, action, performed_by, comments)],
        ["entity_name", "record_id", "action", "performed_by", "comments"]
    ).withColumn("action_ts", current_timestamp())

    df.write.format("delta").mode("append").save(STEWARD_LOG_PATH)
