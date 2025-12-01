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
# Query 13 -> Single_Value_Queries (query INT PK, value DOUBLE)
# -------------------------
print("\n=== Upload Query 13 ===")
df13 = load_parquet("q13_artist_song_overlap") \
    .select(F.col("overlap_count").alias("value")) \
    .withColumn("query", F.lit(13)) \
    .select("query", "value")

# Check if query 13 already exists in the table
existing_df = spark.read.jdbc(mysql_url, "Single_Value_Queries", properties=mysql_props)
existing_query_13 = existing_df.filter(F.col("query") == 13)

if existing_query_13.count() > 0:
    # Update existing record
    print("Query 13 already exists in Single_Value_Queries, updating...")
    # First delete the existing record for query 13
    from pyspark.sql import DataFrame
    import sys
    
    # Create a temporary table with all records except query 13
    other_queries = existing_df.filter(F.col("query") != 13)
    
    # Combine with new query 13 data
    combined_df = other_queries.union(df13)
    
    # Overwrite the entire table with the combined data
    combined_df.write.mode("overwrite").jdbc(mysql_url, "Single_Value_Queries", properties=mysql_props)
    print("Updated Query 13 in Single_Value_Queries")
else:
    # Append new record
    df13.write.mode("append").jdbc(mysql_url, "Single_Value_Queries", properties=mysql_props)
    print("Added Query 13 to Single_Value_Queries")

# -------------------------
# Query 14 -> Average_Artist_Position (artist_id PRIMARY KEY, average DOUBLE)
# -------------------------
print("\n=== Upload Query 14 ===")
df14 = load_parquet("q14_avg_artist_position") \
    .select("artist_id", "average")

df14.write.mode("overwrite").jdbc(mysql_url, "Average_Artist_Position", properties=mysql_props)
print("Uploaded Query 14 -> Average_Artist_Position")

# -------------------------
# Query 15 -> Global_Top_5_Correlates_Top_Per_User (ranking PK, artist_id, users)
# -------------------------
print("\n=== Upload Query 15 ===")
df15 = load_parquet("q15_global_top5_by_sum_rank") \
    .select(F.col("ranking").cast("int"), "artist_id", F.col("users").cast("int"))

df15.write.mode("overwrite").jdbc(mysql_url, "Global_Top_5_Correlates_Top_Per_User", properties=mysql_props)
print("Uploaded Query 15 -> Global_Top_5_Correlates_Top_Per_User")

# -------------------------
# Query 16 -> Same_Top_1_And_2 (composite PK id_artist_position_1, id_artist_position_2, users)
# -------------------------
print("\n=== Upload Query 16 ===")
df16 = load_parquet("q16_same_top1_and_2") \
    .select(
        F.col("id_artist_position_1"),
        F.col("id_artist_position_2"),
        F.col("users").cast("int")
    )

# Ensure column order matches table definition
df16.write.mode("overwrite").jdbc(mysql_url, "Same_Top_1_And_2", properties=mysql_props)
print("Uploaded Query 16 -> Same_Top_1_And_2")

# -------------------------
# Query 18 -> Top_Artists_In_Between_Listeners (ranking PK, artist_id)
# -------------------------
print("\n=== Upload Query 18 ===")
df18 = load_parquet("q18_top_artists_heavy_listeners") \
    .select(F.col("ranking").cast("int"), F.col("artist_id"))

df18.write.mode("overwrite").jdbc(mysql_url, "Top_Artists_In_Between_Listeners", properties=mysql_props)
print("Uploaded Query 18 -> Top_Artists_In_Between_Listeners")

# Finish
spark.stop()
print("\nAll query results uploaded to MySQL.")