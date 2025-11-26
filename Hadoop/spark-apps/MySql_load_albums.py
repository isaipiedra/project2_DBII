from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring

spark = SparkSession.builder \
    .appName("Load Albums to MySQL") \
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

print("\n\n\nReading albums...\n\n\n")

dim_album = spark.read.parquet(DW + "album")

albums_for_mysql = dim_album.select(
    col("album_id").alias("id"),
    substring(col("album_name"), 1, 500).alias("name")
).dropDuplicates(["id"])

albums_for_mysql.write.jdbc(
    url=mysql_url,
    table="Albums",
    mode="append",
    properties=mysql_props
)

print("\n\n\nAlbums inserted successfully!\n\n\n")
spark.stop()
