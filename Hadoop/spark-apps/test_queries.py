from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


spark = SparkSession.builder \
    .appName("TestDW-Splitted") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

DW = "hdfs://namenode:9000/music_dw/"


artist = spark.read.parquet(DW + "artist")
track = spark.read.parquet(DW + "track")
album = spark.read.parquet(DW + "album")
user = spark.read.parquet(DW + "user")

fact_artists = spark.read.parquet(DW + "fact_artists")
fact_tracks  = spark.read.parquet(DW + "fact_tracks")
fact_albums  = spark.read.parquet(DW + "fact_albums")


print("\n=== Conteo de filas en cada tabla ===")
print("artist:", artist.count())
print("track:", track.count())
print("album:", album.count())
print("user:", user.count())
print("fact_artists:", fact_artists.count())
print("fact_tracks:", fact_tracks.count())
print("fact_albums:", fact_albums.count())


print("\n=== 10 filas de fact_artists ===")
fact_artists.show(10, truncate=False)

print("\n=== 10 filas de fact_tracks ===")
fact_tracks.show(10, truncate=False)

print("\n=== 10 filas de fact_albums ===")
fact_albums.show(10, truncate=False)

print("\n=== Top 10 artistas más escuchados ===")
fact_artists.groupBy("artist_id") \
    .agg(sum("playcount_artist").alias("total_plays")) \
    .orderBy(col("total_plays").desc()) \
    .show(10, truncate=False)


print("\n=== Top 10 tracks más escuchados ===")
fact_tracks.groupBy("track_id") \
    .agg(sum("playcount_track").alias("total_plays")) \
    .orderBy(col("total_plays").desc()) \
    .show(10, truncate=False)


print("\n=== Top 10 albums más escuchados ===")
fact_albums.groupBy("album_id") \
    .agg(sum("playcount_album").alias("total_plays")) \
    .orderBy(col("total_plays").desc()) \
    .show(10, truncate=False)

spark.stop()
