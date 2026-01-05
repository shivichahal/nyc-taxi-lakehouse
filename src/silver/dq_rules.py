from pyspark.sql.functions import *

def apply_dq_rules(df):
    return df.withColumn(
        "dq_status",
        when(
            (col("passenger_count") > 0) &
            (col("trip_distance") > 0) &
            (col("total_amount") >= 0) &
            (col("tpep_pickup_datetime") < col("tpep_dropoff_datetime")),
            "PASS"
        ).otherwise("FAIL")
    )
