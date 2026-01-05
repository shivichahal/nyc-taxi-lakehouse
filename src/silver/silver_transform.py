from pyspark.sql.functions import *
from silver.dq_rules import apply_dq_rules
from config.paths import BRONZE_PATH, SILVER_PASS_PATH, SILVER_FAIL_PATH

def build_silver(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df = apply_dq_rules(df)

    # PASS
    df.filter(col("dq_status") == "PASS") \
      .write.format("delta").mode("overwrite").save(SILVER_PASS_PATH)

    # FAIL (with reasons)
    df.filter(col("dq_status") == "FAIL") \
      .write.format("delta").mode("overwrite").save(SILVER_FAIL_PATH)
