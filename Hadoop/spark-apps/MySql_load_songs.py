from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Songs to MySQL") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Paths
DW = "hdfs://namenode:9000/music_dw/"

# MySQL connection settings
mysql_url = "jdbc:mysql://host.docker.internal:3307/metrics"
mysql_props = {
    "user": "root",
    "password": "BDII_PROYII",
    "driver": "com.mysql.cj.jdbc.Driver"
}

print("📖 Reading songs from data warehouse...")

# Read fact or dimension table that contains songs
dim_song = spark.read.parquet(DW + "track")

print(f"✓ Total songs: {dim_song.count()}")

# Prepare songs (id + name only, truncate + dedupe)
songs_for_mysql = dim_song.select(
    col("track_id").alias("id"),
    substring(col("track_name"), 1, 450).alias("name")
).dropDuplicates(["id"])

print(f"✓ Unique songs after deduplication: {songs_for_mysql.count()}")

# Show sample
print("\n📝 Sample data:")
songs_for_mysql.show(5, truncate=False)

# Insert into MySQL
print("\n💾 Inserting songs into MySQL...")
songs_for_mysql.write.jdbc(
    url=mysql_url,
    table="Songs",
    mode="append",
    properties=mysql_props
)

print(f"✅ Successfully inserted {songs_for_mysql.count()} songs!")

spark.stop()
