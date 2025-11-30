from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("Upload Music Results 1-6 to MySQL") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars", "/opt/jars/mysql-connector-j-8.3.0.jar") \
    .getOrCreate()

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

print("\n=== QUERY 1: Top 20 General Artists ===")
df1 = load_parquet("q1_top_artists") \
    .select(
        F.col("ranking").cast("int"),
        "artist_id",
        F.col("listeners").cast("int"),
        F.col("participation_percentage").cast("double")
    )

df1.write.mode("append").jdbc(mysql_url, "Top_20_General_Artists", properties=mysql_props)
print("✅ OK → Top_20_General_Artists")

print("\n=== QUERY 2: Top 20 General Songs ===")
df2 = load_parquet("q2_top_songs") \
    .select(
        F.col("ranking").cast("int"),
        F.col("song_id"),
        F.col("listeners").cast("int"),
        F.col("participation_percentage").cast("double")
    )

df2.write.mode("append").jdbc(mysql_url, "Top_20_General_Songs", properties=mysql_props)
print("✅ OK → Top_20_General_Songs")

print("\n=== QUERY 3: Top 20 General Albums ===")
df3 = load_parquet("q3_top_albums") \
    .select(
        F.col("ranking").cast("int"),
        "album_id",
        F.col("listeners").cast("int"),
        F.col("participation_percentage").cast("double")
    )

df3.write.mode("append").jdbc(mysql_url, "Top_20_General_Albums", properties=mysql_props)
print("✅ OK → Top_20_General_Albums")

print("\n=== QUERY 4: Same Top One Artist ===")
df4 = load_parquet("q4_same_top_artist") \
    .select(
        "artist_id",
        F.col("frequency").cast("int")
    )

df4.write.mode("append").jdbc(mysql_url, "Same_Top_One_Artist", properties=mysql_props)
print("✅ OK → Same_Top_One_Artist")

print("\n=== QUERY 5: Mentions Per Artist ===")
df5 = load_parquet("q5_mentions_artist") \
    .select(
        "artist_id",
        F.col("mentions").cast("int")
    )

df5.write.mode("append").jdbc(mysql_url, "Mentions_Per_Artist", properties=mysql_props)
print("✅ OK → Mentions_Per_Artist")

print("\n=== QUERY 6: Long Tail ===")
df6 = load_parquet("q6_long_tail") \
    .select(
        "artist_id",
        F.col("mentions_percentage").cast("double")
    )

df6.write.mode("append").jdbc(mysql_url, "Long_Tail", properties=mysql_props)
print("✅ OK → Long_Tail")

print("\n=== LOADING METRICS TABLES ===")

metrics_df = load_parquet("q_metrics") \
    .select(
        F.col("query").cast("int"),
        F.col("average").cast("double"),
        F.col("median").cast("double"),
        F.col("standard_deviation").cast("double")
    )

print("Loading metrics for Query 5")
metrics_df.write.mode("append").jdbc(mysql_url, "Metrics", properties=mysql_props)
print("✅ OK → Metrics")

single_value_df = load_parquet("q_single_values") \
    .select(
        F.col("query").cast("int"),
        F.col("value").cast("double")
    )

print("Loading single value for Query 6")
single_value_df.write.mode("append").jdbc(mysql_url, "Single_Value_Queries", properties=mysql_props)
print("✅ OK → Single_Value_Queries")

print("\n=== ✅ FINALIZADO: QUERIES 1-6 Y MÉTRICAS SUBIDAS A MYSQL ===\n")
spark.stop()