from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

spark = SparkSession.builder \
    .appName("Load Artists to MySQL") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

DW = "hdfs://namenode:9000/music_dw/"

mysql_url = "jdbc:mysql://host.docker.internal:3307/metrics"
mysql_props = {
    "user": "root",
    "password": "BDII_PROYII",
    "driver": "com.mysql.cj.jdbc.Driver"
}

print("\n\n\nReading artists...\n\n\n")

dim_artist = spark.read.parquet(DW + "artist")

artists_for_mysql = dim_artist.select(
    col("artist_id").alias("id"),
    substring(col("artist_name"), 1, 500).alias("name")
).dropDuplicates(["id"])

artists_for_mysql.write.jdbc(
    url=mysql_url,
    table="Artists",
    mode="append",
    properties=mysql_props
)

print("Artists inserted successfully!")
spark.stop()
