from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, abs, hash, when
)

#Session de spark
spark = SparkSession.builder \
    .appName("MusicDW Loader SAFE") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

RAW = "hdfs://namenode:9000/music_raw/"
DW  = "hdfs://namenode:9000/music_dw/"

#Generador de keys. Esto es para generar los ids
def clean_key(column):
    return lower(
        trim(
            regexp_replace(
                regexp_replace(column, r'[\u0000-\u001F]', ''),  # control chars
                r'\s+', ' '                                     # collapse spaces
            )
        )
    )

def clean_playcount(colname):
    return when(col(colname).rlike("^[0-9]+$"), col(colname).cast("bigint")).otherwise(None)

# Se cargan los CSV
users_raw = spark.read.csv(RAW + "users.csv", header=True, inferSchema=True)

artists_raw = spark.read.csv(RAW + "user_top_artists.csv", header=True, inferSchema=True) \
    .withColumn("artist_key", clean_key(col("artist_name"))) \
    .withColumn("playcount", clean_playcount("playcount"))

tracks_raw = spark.read.csv(RAW + "user_top_tracks.csv", header=True, inferSchema=True) \
    .withColumn("artist_key", clean_key(col("artist_name"))) \
    .withColumn("track_key", clean_key(col("track_name"))) \
    .withColumn("playcount", clean_playcount("playcount"))

albums_raw = spark.read.csv(RAW + "user_top_albums.csv", header=True, inferSchema=True) \
    .withColumn("artist_key", clean_key(col("artist_name"))) \
    .withColumn("album_key", clean_key(col("album_name"))) \
    .withColumn("playcount", clean_playcount("playcount"))

#Dimensiones con sus nombres
dim_artist = (
    artists_raw.select("artist_key", "artist_name")
    .union(tracks_raw.select("artist_key", "artist_name"))
    .union(albums_raw.select("artist_key", "artist_name"))
    .dropDuplicates(["artist_key"])
    .withColumn("artist_id", abs(hash(col("artist_key"))))
    .select("artist_id", "artist_name", "artist_key")
)

dim_track = (
    tracks_raw.select("track_key", "track_name", "artist_key")
    .dropDuplicates(["track_key", "artist_key"])
    .withColumn("track_id", abs(hash(col("track_key"), col("artist_key"))))
    .select("track_id", "track_name", "artist_key", "track_key")
)

dim_album = (
    albums_raw.select("album_key", "album_name", "artist_key")
    .dropDuplicates(["album_key", "artist_key"])
    .withColumn("album_id", abs(hash(col("album_key"), col("artist_key"))))
    .select("album_id", "album_name", "artist_key", "album_key")
)

dim_user = users_raw.select("user_id", "country", "total_scrobbles").dropDuplicates()

# Tablas de metricas
fact_artists = (
    artists_raw.join(dim_artist, "artist_key", "left")
    .select("user_id", "artist_id", "rank", col("playcount").alias("playcount_artist"))
)

fact_tracks = (
    tracks_raw.join(dim_track, ["track_key", "artist_key"], "left")
    .join(dim_artist, "artist_key", "left")
    .select("user_id", "track_id", "artist_id", "rank", col("playcount").alias("playcount_track"))
)

fact_albums = (
    albums_raw.join(dim_album, ["album_key", "artist_key"], "left")
    .join(dim_artist, "artist_key", "left")
    .select("user_id", "album_id", "artist_id", col("playcount").alias("playcount_album"))
)

# Se guardan los dw en sus respectivas zonas
dim_artist.write.mode("overwrite").parquet(DW + "artist")
dim_track.write.mode("overwrite").parquet(DW + "track")
dim_album.write.mode("overwrite").parquet(DW + "album")
dim_user.write.mode("overwrite").parquet(DW + "user")

fact_artists.write.mode("overwrite").parquet(DW + "fact_artists")
fact_tracks.write.mode("overwrite").parquet(DW + "fact_tracks")
fact_albums.write.mode("overwrite").parquet(DW + "fact_albums")

spark.stop()
print("🎉 DW generado SIN pérdida de información")
