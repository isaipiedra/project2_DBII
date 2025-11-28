from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# =========================================
#  SPARK SESSION
# =========================================
spark = SparkSession.builder \
    .appName("Upload Music Results to MySQL") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars", "/opt/jars/mysql-connector-j-8.3.0.jar") \
    .getOrCreate()

# =========================================
#  MYSQL CONNECTION
# =========================================
mysql_url = "jdbc:mysql://host.docker.internal:3307/metrics"
mysql_props = {
    "user": "root",
    "password": "BDII_PROYII",
    "driver": "com.mysql.cj.jdbc.Driver"
}

BASE = "/opt/spark-apps/music_results"


# =========================================
# Helper
# =========================================
def load_parquet(name):
    path = f"{BASE}/{name}"
    print(f"--> Loading: {path}")
    return spark.read.parquet(path)



# ============= QUERY 7 =============
df7 = load_parquet("q7_items_per_user") \
    .select(
        F.col("user_id").cast("int"),
        F.col("total_items").alias("items").cast("int")
    )

df7.write.jdbc(mysql_url, "Items_Per_User", "append", mysql_props)



# ============= QUERY 8 =============
df8 = load_parquet("q8_unique_items") \
    .select(
        F.col("user_id").cast("int"),
        F.col("amount_artists").alias("artists").cast("int"),
        F.col("amount_tracks").alias("songs").cast("int"),
        F.col("amount_albums").alias("albums").cast("int")
    )

df8.write.jdbc(mysql_url, "Unique_Items", "append", mysql_props)



# ============= QUERY 9 (Artistas) =============
df9_art = load_parquet("q9_artists") \
    .select(
        F.col("Ranking_triple").alias("ranking").cast("int"), 
        "artist_id_1", 
        "artist_id_2", 
        "artist_id_3", 
        F.col("total_users").cast("int")
    )

df9_art.write.jdbc(mysql_url, "top_10_Duplicated_Artists", "append", mysql_props)



# ============= QUERY 9 (Álbumes) =============
df9_alb = load_parquet("q9_albums") \
    .select(
        F.col("Ranking_triple").alias("ranking").cast("int"),  
        "album_id_1", 
        "album_id_2", 
        "album_id_3", 
        F.col("total_users").cast("int")
    )

df9_alb.write.jdbc(mysql_url, "top_10_Duplicated_Albums", "append", mysql_props)



# ============= QUERY 9 (Tracks) =============
df9_songs = load_parquet("q9_tracks") \
    .select(
        F.col("ranking_triple").alias("ranking").cast("int"),
        F.col("track_id_1").alias("song_id_1"), 
        F.col("track_id_2").alias("song_id_2"),
        F.col("track_id_3").alias("song_id_3"),
        F.col("total_users").cast("int")
    )

df9_songs.write.jdbc(mysql_url, "top_10_Duplicated_Songs", "append", mysql_props)



# ============= QUERY 10 =============
df10 = load_parquet("q10_loyal_listeners") \
    .select(
        "artist_id",
        F.col("loyal_user_count").alias("loyal_listeners").cast("int")
    )

df10.write.jdbc(mysql_url, "Loyal_Listeners", "append", mysql_props)



# ============= QUERY 11 (Pairs) =============

df11 = load_parquet("top_artist_pairs") \
    .select(
        F.col("ranking").cast("int"),
        F.col("artist_1").alias("artist_1_id"),
        F.col("artist_2").alias("artist_2_id"),
        F.col("total_users").cast("int")
    )

df11.write.jdbc(mysql_url, "top_50_Paired_Artists", "append", mysql_props)
print("OK → top_50_Paired_Artists")



# ============= QUERY 12 (Trios) =============
df12 = load_parquet("q12_artist_trios") \
    .select(
        F.col("ranking").cast("int"),
        F.col("artist_1").alias("artist_1_id"),
        F.col("artist_2").alias("artist_2_id"),
        F.col("artist_3").alias("artist_3_id"),
        F.col("total_users").cast("int")
    )

df12.write.jdbc(mysql_url, "top_20_Trio_Artists", "append", mysql_props)



spark.stop()