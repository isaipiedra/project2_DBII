# load_queries_13_to_18.py
# Load Parquet results -> write to MySQL existing tables matching your schema

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

spark = SparkSession.builder \
    .appName("Load Queries 13-18 into MySQL") \
    .config("spark.jars", "/opt/jars/mysql-connector-j-8.3.0.jar") \
    .getOrCreate()

mysql_url = "jdbc:mysql://host.docker.internal:3307/metrics"
mysql_props = {"user": "root", "password": "BDII_PROYII", "driver": "com.mysql.cj.jdbc.Driver"}

BASE = "/opt/spark-apps/music_results"

def load_parquet(path_name):
    path = os.path.join(BASE, path_name)
    print("--> Loading:", path)
    return spark.read.parquet(path)



# -------------------------
# Query 15 -> Global_Top_5_Correlates_Top_Per_User (ranking PK, artist_id, users)
# -------------------------
print("\n=== Upload Query 15 ===")
df15 = load_parquet("q15_global_top5_by_sum_rank") \
    .select(F.col("ranking").cast("int"), "artist_id", F.col("users").cast("int"))

df15.write.mode("overwrite").jdbc(mysql_url, "Global_Top_5_Correlates_Top_Per_User", properties=mysql_props)
print("Uploaded Query 15 -> Global_Top_5_Correlates_Top_Per_User")

# Finish
spark.stop()
print("\nAll query results uploaded to MySQL.")
