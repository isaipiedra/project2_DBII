# diagnostic.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Diagnostic") \
    .config("spark.jars", "/opt/jars/mysql-connector-j-8.3.0.jar") \
    .getOrCreate()

mysql_url = "jdbc:mysql://host.docker.internal:3307/metrics"
mysql_props = {"user": "root", "password": "BDII_PROYII", "driver": "com.mysql.cj.jdbc.Driver"}

# Check what's in Artists table
artists_mysql = spark.read.jdbc(mysql_url, "Artists", properties=mysql_props)
print("MySQL Artists table schema:")
artists_mysql.printSchema()
print("\nFirst 5 artists in MySQL:")
artists_mysql.show(5, truncate=False)

# Check DW artists
DW = "hdfs://namenode:9000/music_dw/"
dim_artist = spark.read.parquet(DW + "artist")
print("\nDW Artists schema:")
dim_artist.printSchema()
print("\nFirst 5 artists in DW:")
dim_artist.show(5, truncate=False)

spark.stop()