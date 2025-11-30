from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# ========================================
# SPARK SESSION
# ========================================
spark = SparkSession.builder \
    .appName("Upload Music Results (19-23) to MySQL") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars", "/opt/jars/mysql-connector-j-8.3.0.jar") \
    .getOrCreate()

# ========================================
# MYSQL CONNECTION
# ========================================
mysql_url = "jdbc:mysql://host.docker.internal:3307/metrics"
mysql_props = {
    "user": "root",
    "password": "BDII_PROYII",
    "driver": "com.mysql.cj.jdbc.Driver"
}

BASE = "/opt/spark-apps/music_results"

def load_parquet(name):
    path = f"{BASE}/{name}"
    return spark.read.parquet(path)


# QUERY 19: Cross Popularity
df19 = load_parquet("q19_cross_popularity") \
    .select(
        "artist_id",
        F.col("song_frequency").cast("int"),
        F.col("artist_frequency").cast("int"),
        F.col("difference").cast("int")
    )

df19.write.mode("overwrite").jdbc(mysql_url, "Cross_Popularity", properties=mysql_props)

# QUERY 20: Diverse Artists
df20 = load_parquet("q20_diverse_artists") \
    .select(
        "artist_id",
        F.col("listeners").cast("int"),
        F.col("songs").cast("int")
    )

df20.write.mode("overwrite").jdbc(mysql_url, "Diverse_Artists", properties=mysql_props)

# QUERY 21: Missing Data Count
df21 = load_parquet("q21_missing_data_count")


value_21 = df21.collect()[0]["total_users_with_issues"]

df21_insert = spark.createDataFrame(
    [(21, int(value_21))],
    ["query", "value"]
)

# Insertar en Single_Value_Queries
df21_insert.write.mode("append").jdbc(mysql_url, "Single_Value_Queries", properties=mysql_props)


# QUERY 22: Outlier Users Count
df22 = load_parquet("q22_outlier_users_count")


value_22 = df22.collect()[0]["outlier_count"]

df22_insert = spark.createDataFrame(
    [(22, int(value_22))],
    ["query", "value"]
)

# Insertar en Single_Value_Queries
df22_insert.write.mode("append").jdbc(mysql_url, "Single_Value_Queries", properties=mysql_props)


# QUERY 23: Low Coverage Artists
print("\n=== QUERY 23: Low Coverage Artists ===")
df23 = load_parquet("q23_low_coverage_artists") \
    .select(
        "artist_id",
        F.col("appearances").cast("int")
    )

# Insertar la tabla completa
df23.write.mode("overwrite").jdbc(mysql_url, "Low_Coverage_Artists", properties=mysql_props)

# Insertar el conteo total en Single_Value_Queries
value_23 = df23.count()
df23_insert = spark.createDataFrame(
    [(23, int(value_23))],
    ["query", "value"]
)

df23_insert.write.mode("append").jdbc(mysql_url, "Single_Value_Queries", properties=mysql_props)


spark.stop()