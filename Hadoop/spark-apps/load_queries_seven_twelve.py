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

def load_parquet(name):
    path = f"{BASE}/{name}"
    print(f"--> Loading: {path}")
    return spark.read.parquet(path)


# =========================================================
# ============= QUERY 7 ===================================
# =========================================================
print("\n=== QUERY 7: Items Per User ===")
df7 = load_parquet("q7_items_per_user") \
    .select(
        F.col("user_id").cast("int"),
        F.col("total_items").alias("items").cast("int")
    )

df7.write.mode("overwrite").jdbc(mysql_url, "Items_Per_User", properties=mysql_props)
print("OK → Items_Per_User")


# =========================================================
# ============= QUERY 8 ===================================
# =========================================================
print("\n=== QUERY 8: Unique Items ===")
df8 = load_parquet("q8_unique_items") \
    .select(
        F.col("user_id").cast("int"),
        F.col("amount_artists").alias("artists").cast("int"),
        F.col("amount_tracks").alias("songs").cast("int"),
        F.col("amount_albums").alias("albums").cast("int")
    )

df8.write.mode("overwrite").jdbc(mysql_url, "Unique_Items", properties=mysql_props)
print("OK → Unique_Items")


# =========================================================
# ============= QUERY 9 ARTISTS ===========================
# =========================================================
print("\n=== QUERY 9: Duplicated Artists ===")
df9_art = load_parquet("q9_artists") \
    .withColumn("ranking", F.col("Ranking_triple").cast("int")) \
    .select(
        "ranking",
        "artist_id_1",
        "artist_id_2",
        "artist_id_3",
        F.col("total_users").cast("int")
    )

df9_art.write.mode("overwrite").jdbc(mysql_url, "top_10_Duplicated_Artists", properties=mysql_props)
print("OK → top_10_Duplicated_Artists")


# =========================================================
# ============= QUERY 9 ALBUMS ============================
# =========================================================
print("\n=== QUERY 9: Duplicated Albums ===")
df9_alb = load_parquet("q9_albums") \
    .withColumn("ranking", F.col("Ranking_triple").cast("int")) \
    .select(
        "ranking",
        "album_id_1",
        "album_id_2",
        "album_id_3",
        F.col("total_users").cast("int")
    )

df9_alb.write.mode("overwrite").jdbc(mysql_url, "top_10_Duplicated_Albums", properties=mysql_props)
print("OK → top_10_Duplicated_Albums")


# =========================================================
# ============= QUERY 9 TRACKS ============================
# =========================================================
print("\n=== QUERY 9: Duplicated Tracks ===")
df9_songs = load_parquet("q9_tracks") \
    .withColumn("ranking", F.col("ranking_triple").cast("int")) \
    .select(
        "ranking",
        F.col("track_id_1").alias("song_id_1"),
        F.col("track_id_2").alias("song_id_2"),
        F.col("track_id_3").alias("song_id_3"),
        F.col("total_users").cast("int")
    )

df9_songs.write.mode("overwrite").jdbc(mysql_url, "top_10_Duplicated_Songs", properties=mysql_props)
print("OK → top_10_Duplicated_Songs")


# =========================================================
# ============= QUERY 10 ==================================
# =========================================================
print("\n=== QUERY 10: Loyal Listeners ===")
df10 = load_parquet("q10_loyal_listeners") \
    .select(
        "artist_id",
        F.col("loyal_user_count").alias("loyal_listeners").cast("int")
    )

df10.write.mode("overwrite").jdbc(mysql_url, "Loyal_Listeners", properties=mysql_props)
print("OK → Loyal_Listeners")


# =========================================================
# ============= QUERY 11 (Pairs) ==========================
# =========================================================
print("\n=== QUERY 11: Paired Artists ===")
df11 = load_parquet("q11_artist_pairs") \
    .select(
        F.col("ranking").cast("int"),
        F.col("artist_1").alias("artist_1_id"),
        F.col("artist_2").alias("artist_2_id"),
        F.col("total_users").cast("int")
    )

df11.write.mode("overwrite").jdbc(mysql_url, "top_50_Paired_Artists", properties=mysql_props)
print("OK → top_50_Paired_Artists")


# =========================================================
# ============= QUERY 12 (Trios) ==========================
# =========================================================
print("\n=== QUERY 12: Trio Artists ===")
df12 = load_parquet("q12_artist_trios") \
    .select(
        F.col("ranking").cast("int"),
        F.col("artist_1").alias("artist_1_id"),
        F.col("artist_2").alias("artist_2_id"),
        F.col("artist_3").alias("artist_3_id"),
        F.col("total_users").cast("int")
    )

df12.write.mode("overwrite").jdbc(mysql_url, "top_20_Trio_Artists", properties=mysql_props)
print("OK → top_20_Trio_Artists")

print("\n=== FINALIZADO: TODOS LOS PARQUETS SUBIDOS A MYSQL ===\n")
spark.stop()