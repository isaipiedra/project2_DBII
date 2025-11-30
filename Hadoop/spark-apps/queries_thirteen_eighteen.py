# compute_queries_13_to_18.py
# Load DW Parquet -> register SQL views -> run queries 13-18 in pure SQL -> save Parquet results

from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("Compute Queries 13-18 (SQL)") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

DW = "hdfs://namenode:9000/music_dw/"
OUT_BASE = "/opt/spark-apps/music_results"

os.makedirs(OUT_BASE, exist_ok=True)

# -------------------------
# Load parquet DW tables and register as SQL temp views
# -------------------------
fact_artists = spark.read.parquet(DW + "fact_artists")
fact_tracks  = spark.read.parquet(DW + "fact_tracks")
fact_albums  = spark.read.parquet(DW + "fact_albums")
dim_user     = spark.read.parquet(DW + "user")
dim_artist   = spark.read.parquet(DW + "artist")
dim_track    = spark.read.parquet(DW + "track")
dim_album    = spark.read.parquet(DW + "album")

fact_artists.createOrReplaceTempView("fact_artists")
fact_tracks.createOrReplaceTempView("fact_tracks")
fact_albums.createOrReplaceTempView("fact_albums")
dim_user.createOrReplaceTempView("dim_user")
dim_artist.createOrReplaceTempView("dim_artist")
dim_track.createOrReplaceTempView("dim_track")
dim_album.createOrReplaceTempView("dim_album")



# -------------------------
# Query 15 - Global top-5 ranked by SUM(rank), and users = distinct listeners count
# Output columns: ranking (1..5), artist_id, users
# -------------------------
query15 = """
WITH artist_scores AS (
    SELECT
        artist_id,
        SUM(1.0 / rank) AS score,
        COUNT(DISTINCT user_id) AS listeners
    FROM fact_artists
    WHERE artist_id IS NOT NULL AND rank IS NOT NULL
    GROUP BY artist_id
),

top5 AS (
    SELECT
        artist_id,
        score,
        ROW_NUMBER() OVER (ORDER BY score DESC) AS ranking
    FROM artist_scores
    ORDER BY score DESC
    LIMIT 5
),

users_top1 AS (
    SELECT
        artist_id,
        COUNT(*) AS users
    FROM fact_artists
    WHERE rank = 1
    GROUP BY artist_id
)

SELECT
    t.ranking,
    t.artist_id,
    COALESCE(u.users, 0) AS users
FROM top5 t
LEFT JOIN users_top1 u
    ON t.artist_id = u.artist_id
ORDER BY ranking;
"""
df15 = spark.sql(query15)
df15.write.mode("overwrite").parquet(os.path.join(OUT_BASE, "q15_global_top5_by_sum_rank"))
print("Saved q15_global_top5_by_sum_rank")


# Finish
spark.stop()
print("All queries (13,14,15,16,18) finished and saved to:", OUT_BASE)
