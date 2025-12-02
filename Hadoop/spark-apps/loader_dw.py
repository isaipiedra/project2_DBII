from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, sha2, concat_ws, when
)

# Sesión de spark
spark = SparkSession.builder \
    .appName("MusicDW Loader - SHA256 SAFE") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

RAW = "hdfs://namenode:9000/music_raw/"
DW  = "hdfs://namenode:9000/music_dw/"

# Generadores de keys unicos con el artista usando sha256
def artist_key(col_name):
    return sha2(trim(lower(col(col_name))), 256)

def track_key():
    return sha2(concat_ws("::",
            trim(lower(col("track_name"))),
            trim(lower(col("artist_name")))
        ), 256)

def album_key():
    return sha2(concat_ws("::",
            trim(lower(col("album_name"))),
            trim(lower(col("artist_name")))
        ), 256)

def clean_playcount(c):
    return when(col(c).rlike("^[0-9]+$"), col(c).cast("bigint")).otherwise(None)


# Cargar tablas del CSV
users_raw = spark.read.csv(RAW + "users.csv", header=True, inferSchema=True)

artists_raw = spark.read.csv(RAW + "user_top_artists.csv", header=True, inferSchema=True) \
    .withColumn("artist_key", artist_key("artist_name")) \
    .withColumn("playcount", clean_playcount("playcount"))

tracks_raw = spark.read.csv(RAW + "user_top_tracks.csv", header=True, inferSchema=True) \
    .withColumn("artist_key", artist_key("artist_name")) \
    .withColumn("track_key", track_key()) \
    .withColumn("playcount", clean_playcount("playcount"))

albums_raw = spark.read.csv(RAW + "user_top_albums.csv", header=True, inferSchema=True) \
    .withColumn("artist_key", artist_key("artist_name")) \
    .withColumn("album_key", album_key()) \
    .withColumn("playcount", clean_playcount("playcount"))


# tablas de dimensiones
dim_artist = (
    artists_raw.select("artist_key", "artist_name")
    .union(tracks_raw.select("artist_key", "artist_name"))
    .union(albums_raw.select("artist_key", "artist_name"))
    .dropDuplicates(["artist_key"])
    .withColumn("artist_id", col("artist_key"))
    .selectExpr("artist_id", "artist_name", "artist_key")
)

dim_track = (
    tracks_raw.select("track_key", "track_name", "artist_key")
    .dropDuplicates(["track_key"])
    .withColumn("track_id", col("track_key"))
)

dim_album = (
    albums_raw.select("album_key", "album_name", "artist_key")
    .dropDuplicates(["album_key"])
    .withColumn("album_id", col("album_key"))
)

dim_user = users_raw.select("user_id", "country", "total_scrobbles").dropDuplicates()


# Tablas de métricas
fact_artists = (
    artists_raw.join(dim_artist, "artist_key", "left")
    .select(
        "user_id",
        "artist_id",
        "rank",
        col("playcount").alias("playcount_artist")
    )
)

fact_tracks = (
    tracks_raw.join(dim_track, ["track_key", "artist_key"], "left")
    .join(dim_artist, "artist_key", "left")
    .select(
        "user_id",
        "track_id",
        "artist_id",
        "rank",
        col("playcount").alias("playcount_track")
    )
)

fact_albums = (
    albums_raw.join(dim_album, ["album_key", "artist_key"], "left")
    .join(dim_artist, "artist_key", "left")
    .select(
        "user_id",
        "album_id",
        "artist_id",
        "rank",
        col("playcount").alias("playcount_album")
    )
)

# Guardar los paquetes PARQUET en el HDFS
dim_artist.write.mode("overwrite").parquet(DW + "artist")
dim_track.write.mode("overwrite").parquet(DW + "track")
dim_album.write.mode("overwrite").parquet(DW + "album")
dim_user.write.mode("overwrite").parquet(DW + "user")

fact_artists.write.mode("overwrite").parquet(DW + "fact_artists")
fact_tracks.write.mode("overwrite").parquet(DW + "fact_tracks")
fact_albums.write.mode("overwrite").parquet(DW + "fact_albums")

spark.stop()
