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
# Query 13 - Artist-Song Overlap
# Count of users where user's #1 artist == artist of user's #1 track
# Result -> single value parquet
# -------------------------
query13 = """
WITH top_artist AS (
  SELECT user_id, artist_id
  FROM fact_artists
  WHERE rank = 1 AND artist_id IS NOT NULL
),
top_track AS (
  SELECT user_id, artist_id
  FROM fact_tracks
  WHERE rank = 1 AND artist_id IS NOT NULL
)
SELECT COUNT(*) AS overlap_count
FROM top_artist tA
JOIN top_track tT
  ON tA.user_id = tT.user_id
 WHERE tA.artist_id = tT.artist_id
"""
df13 = spark.sql(query13)
df13.write.mode("overwrite").parquet(os.path.join(OUT_BASE, "q13_artist_song_overlap"))
print("Saved q13_artist_song_overlap")

# -------------------------
# Query 14 - Average Artist Position
# For each artist compute average(rank) across users that include that artist
# Output columns: artist_id, average
# -------------------------
query14 = """
SELECT
  artist_id,
  AVG(rank) AS average
FROM fact_artists
WHERE artist_id IS NOT NULL AND rank IS NOT NULL
GROUP BY artist_id
"""
df14 = spark.sql(query14)
df14.write.mode("overwrite").parquet(os.path.join(OUT_BASE, "q14_avg_artist_position"))
print("Saved q14_avg_artist_position")

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

# -------------------------
# Query 16 - Same Top 1 and Top 2 (ordered pairs)
# Output columns: id_artist_position_1, id_artist_position_2, users
# -------------------------
query16 = """
WITH user_top2 AS (
  SELECT
    user_id,
    MAX(CASE WHEN rank = 1 THEN artist_id END) AS id_artist_position_1,
    MAX(CASE WHEN rank = 2 THEN artist_id END) AS id_artist_position_2
  FROM fact_artists
  WHERE rank <= 2
  GROUP BY user_id
)
SELECT
  id_artist_position_1,
  id_artist_position_2,
  COUNT(*) AS users
FROM user_top2
WHERE id_artist_position_1 IS NOT NULL AND id_artist_position_2 IS NOT NULL
GROUP BY id_artist_position_1, id_artist_position_2
ORDER BY users DESC
"""
df16 = spark.sql(query16)
df16.write.mode("overwrite").parquet(os.path.join(OUT_BASE, "q16_same_top1_and_2"))
print("Saved q16_same_top1_and_2")

# -------------------------
# Query 18 - Top artists among heavy listeners
# heavy listeners = users with >40 DISTINCT tracks in fact_tracks
# For each heavy listener consider only their top 10 artists (rank 1..10 in fact_artists)
# Count how many heavy listeners have each artist in their top 10
# Output columns: ranking, artist_id
# -------------------------
query18 = """
WITH heavy_listeners AS (
  SELECT user_id
  FROM fact_tracks
  WHERE track_id IS NOT NULL
  GROUP BY user_id
  HAVING COUNT(DISTINCT track_id) > 40
),
heavy_top10 AS (
  SELECT DISTINCT fa.user_id, fa.artist_id
  FROM fact_artists fa
  JOIN heavy_listeners hl ON fa.user_id = hl.user_id
  WHERE fa.rank BETWEEN 1 AND 10 AND fa.artist_id IS NOT NULL
),
artist_counts AS (
  SELECT artist_id, COUNT(DISTINCT user_id) AS heavy_listener_count
  FROM heavy_top10
  GROUP BY artist_id
)
SELECT
  ROW_NUMBER() OVER (ORDER BY heavy_listener_count DESC) AS ranking,
  artist_id
FROM artist_counts
ORDER BY heavy_listener_count DESC
LIMIT 10
"""
df18 = spark.sql(query18)
df18.write.mode("overwrite").parquet(os.path.join(OUT_BASE, "q18_top_artists_heavy_listeners"))
print("Saved q18_top_artists_heavy_listeners")

# Finish
spark.stop()
print("All queries (13,14,15,16,18) finished and saved to:", OUT_BASE)
