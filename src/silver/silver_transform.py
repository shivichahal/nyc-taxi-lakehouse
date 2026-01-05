from pyspark.sql.functions import *
from silver.dq_rules import apply_dq_rules
from config.paths import BRONZE_PATH, SILVER_PATH

def build_silver(spark):
    df = spark.read.format("delta").load(BRONZE_PATH)
    df = apply_dq_rules(df).filter(col("dq_status") == "PASS")
    df.write.format("delta").mode("overwrite").save(SILVER_PATH)
