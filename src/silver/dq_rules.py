from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, StringType

def apply_dq_rules(df):

    return (
        df
        # Build array of failure reasons
        .withColumn(
            "dq_failure_reasons",
            array_remove(array(
                when(col("tpep_pickup_datetime").isNull(), "MISSING_PICKUP_TIME"),
                when(col("tpep_dropoff_datetime").isNull(), "MISSING_DROPOFF_TIME"),
                when(col("passenger_count") <= 0, "INVALID_PASSENGER_COUNT"),
                when(col("trip_distance") <= 0, "INVALID_DISTANCE"),
                when(col("fare_amount") < 0, "NEGATIVE_FARE"),
                when(col("total_amount") < 0, "NEGATIVE_TOTAL"),
                when(
                    col("tpep_pickup_datetime") >= col("tpep_dropoff_datetime"),
                    "PICKUP_AFTER_DROPOFF"
                )
            ), None)
        )

        # Overall status
        .withColumn(
            "dq_status",
            when(size(col("dq_failure_reasons")) == 0, "PASS")
            .otherwise("FAIL")
        )
    )
