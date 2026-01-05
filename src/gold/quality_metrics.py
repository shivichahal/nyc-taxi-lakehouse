from pyspark.sql.functions import *
from config.paths import (
    SILVER_PASS_PATH,
    SILVER_FAIL_PATH,
    MDM_PATH,
    GOLD_PATH
)

# =========================================================
#  COMPLETENESS
# =========================================================
def build_quality_completeness(spark):
    """
    Completeness → Can we fulfill analytics & reporting?
    """

    df = spark.read.format("delta").load(SILVER_PASS_PATH)

    completeness_df = (
        df.groupBy(to_date("tpep_pickup_datetime").alias("date"))
        .agg(
            count("*").alias("total_records"),
            sum(
                when(
                    col("PULocationID").isNotNull() &
                    col("DOLocationID").isNotNull(),
                    1
                ).otherwise(0)
            ).alias("complete_records")
        )
        .withColumn(
            "completeness_pct",
            round(col("complete_records") / col("total_records") * 100, 2)
        )
        .withColumn("dimension", lit("COMPLETENESS"))
    )

    completeness_df.write.format("delta") \
        .mode("overwrite") \
        .save(f"{GOLD_PATH}/completeness")


# =========================================================
#  ACCURACY
# =========================================================
def build_quality_accuracy(spark):
    """
    Accuracy → Are we billing correctly?
    """

    pass_df = spark.read.format("delta").load(SILVER_PASS_PATH)
    fail_df = spark.read.format("delta").load(SILVER_FAIL_PATH)

    accuracy_df = (
        pass_df.groupBy(to_date("tpep_pickup_datetime").alias("date"))
        .count()
        .withColumnRenamed("count", "accurate_records")
        .join(
            fail_df.groupBy(to_date("tpep_pickup_datetime").alias("date"))
            .count()
            .withColumnRenamed("count", "inaccurate_records"),
            ["date"],
            "left"
        )
        .fillna(0)
        .withColumn(
            "accuracy_pct",
            round(
                col("accurate_records") /
                (col("accurate_records") + col("inaccurate_records")) * 100,
                2
            )
        )
        .withColumn("dimension", lit("ACCURACY"))
    )

    accuracy_df.write.format("delta") \
        .mode("overwrite") \
        .save(f"{GOLD_PATH}/accuracy")


# =========================================================
#  TIMELINESS
# =========================================================
def build_quality_timeliness(spark):
    """
    Timeliness → Are decisions based on current data?
    """

    df = spark.read.format("delta").load(SILVER_PASS_PATH)

    timeliness_df = (
        df.withColumn(
            "ingestion_delay_hours",
            (
                unix_timestamp(current_timestamp()) -
                unix_timestamp(col("tpep_pickup_datetime"))
            ) / 3600
        )
        .groupBy(to_date("tpep_pickup_datetime").alias("date"))
        .agg(
            avg("ingestion_delay_hours").alias("avg_delay_hours")
        )
        .withColumn("dimension", lit("TIMELINESS"))
    )

    timeliness_df.write.format("delta") \
        .mode("overwrite") \
        .save(f"{GOLD_PATH}/timeliness")


# =========================================================
# CONSISTENCY
# =========================================================
def build_quality_consistency(spark):
    """
    Consistency → Do transactions align with master data?
    """

    silver_df = spark.read.format("delta").load(SILVER_PASS_PATH)
    mdm_df = spark.read.format("delta").load(MDM_PATH)

    consistency_df = (
        silver_df.join(
            mdm_df,
            silver_df.PULocationID == mdm_df.location_id,
            "left"
        )
        .groupBy(to_date("tpep_pickup_datetime").alias("date"))
        .agg(
            count("*").alias("total_records"),
            sum(
                when(col("location_id").isNotNull(), 1).otherwise(0)
            ).alias("consistent_records")
        )
        .withColumn(
            "consistency_pct",
            round(col("consistent_records") / col("total_records") * 100, 2)
        )
        .withColumn("dimension", lit("CONSISTENCY"))
    )

    consistency_df.write.format("delta") \
        .mode("overwrite") \
        .save(f"{GOLD_PATH}/consistency")
