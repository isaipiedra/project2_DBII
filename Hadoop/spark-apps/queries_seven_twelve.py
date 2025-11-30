from metrics_inserts import init_spark_session
from itertools import combinations
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from metrics_inserts import (
    init_spark_session,
    insert_metrics,
    insert_single_value
)
from pyspark.sql import SparkSession




def load_tables(spark):
    DW = "hdfs://namenode:9000/music_dw/"

    dim_user = spark.read.parquet(DW+"user")
    dim_artist = spark.read.parquet(DW + "artist")
    dim_track  = spark.read.parquet(DW + "track")
    dim_album  = spark.read.parquet(DW + "album")

    fact_artists = spark.read.parquet(DW + "fact_artists")
    fact_tracks  = spark.read.parquet(DW + "fact_tracks")
    fact_albums  = spark.read.parquet(DW + "fact_albums")

    # Crear vistas SQL
    dim_user.createOrReplaceTempView("dim_user")
    dim_artist.createOrReplaceTempView("dim_artist")
    dim_track.createOrReplaceTempView("dim_track")
    dim_album.createOrReplaceTempView("dim_album")

    fact_artists.createOrReplaceTempView("fact_artists")
    fact_tracks.createOrReplaceTempView("fact_tracks")
    fact_albums.createOrReplaceTempView("fact_albums")

#Question 7
def q_items_usuario(spark):
    sql_query = sql_query = """
    WITH user_counts AS (
        SELECT 
            user_id,
            COUNT(*) AS item_count
        FROM fact_artists
        WHERE artist_id IS NOT NULL
        GROUP BY user_id
        
        UNION ALL
        
        SELECT 
            user_id,
            COUNT(*) AS item_count
        FROM fact_tracks
        WHERE track_id IS NOT NULL
        GROUP BY user_id
        
        UNION ALL
        
        SELECT 
            user_id,
            COUNT(*) AS item_count
        FROM fact_albums
        WHERE album_id IS NOT NULL
        GROUP BY user_id
    )
    
    SELECT 
        user_id,
        SUM(item_count) AS total_items
    FROM user_counts
    GROUP BY user_id
    ORDER BY total_items ASC
    """
    return spark.sql(sql_query)

#Question metrics 
def q_avg_med_std(spark):
    sql_query = """
    WITH items AS (
            WITH user_counts AS (
            SELECT 
                user_id,
                COUNT(*) AS item_count
            FROM fact_artists
            WHERE artist_id IS NOT NULL
            GROUP BY user_id
            
            UNION ALL
            
            SELECT 
                user_id,
                COUNT(*) AS item_count
            FROM fact_tracks
            WHERE track_id IS NOT NULL
            GROUP BY user_id
            
            UNION ALL
            
            SELECT 
                user_id,
                COUNT(*) AS item_count
            FROM fact_albums
            WHERE album_id IS NOT NULL
            GROUP BY user_id
        )
        
        SELECT 
            user_id,
            SUM(item_count) AS total_items
        FROM user_counts
        GROUP BY user_id
        ORDER BY total_items ASC
    )
    
    SELECT AVG(total_items) as average, percentile_approx(total_items, 0.5) AS median,
    stddev(total_items) AS standard_deviation
    FROM items
    """

    return spark.sql(sql_query)

#Question 8
def q_unique_elements(spark):
    sql_query = """
    WITH unique_artists AS( 
        SELECT user_id,  COUNT(DISTINCT (artist_id)) as amount_artists
        FROM fact_artists
        WHERE artist_id IS NOT NULL
        GROUP BY user_id
        ), 
        unique_tracks AS(
        SELECT user_id, COUNT(DISTINCT(track_id)) AS amount_tracks
        FROM fact_tracks
        WHERE track_id IS NOT NULL
        GROUP BY user_id
        ), 
        unique_albums AS (
        SELECT user_id, COUNT(DISTINCT(album_id)) as amount_albums
        FROM fact_albums
        WHERE album_id IS NOT NULL
        GROUP BY user_id
        )

        SELECT u.user_id, COALESCE(a.amount_artists, 0) AS amount_artists, COALESCE(t.amount_tracks, 0) AS amount_tracks,
        COALESCE(al.amount_albums,0) AS amount_albums
        FROM  dim_user u
        LEFT JOIN 
        unique_artists a ON u.user_id = a.user_id
        LEFT JOIN 
            unique_tracks t ON u.user_id = t.user_id
        LEFT JOIN unique_albums al ON u.user_id = al.user_id
        ORDER BY
            u.user_id ASC

    """
    return spark.sql(sql_query)

#Question 9
def q_duplicated_artists(spark):
    sql_query =""" 
    WITH UserTriplets AS (
        SELECT
            user_id,
            MAX(CASE WHEN rank = 1 THEN artist_id END) AS artist_id_1,
            MAX(CASE WHEN rank = 2 THEN artist_id END) AS artist_id_2,
            MAX(CASE WHEN rank = 3 THEN artist_id END) AS artist_id_3
        FROM 
            fact_artists
        WHERE 
            rank <= 3
        GROUP BY 
            user_id
    )
    SELECT 
        artist_id_1, artist_id_2, artist_id_3, COUNT(*) AS total_users, RANK() OVER(ORDER BY COUNT(*) DESC) AS Ranking_triple
    FROM 
        UserTriplets
    GROUP BY
        artist_id_1,
        artist_id_2,
        artist_id_3
    HAVING
        COUNT(*) > 1
    ORDER BY
        total_users DESC
    LIMIT 10;
    """
    return spark.sql(sql_query)

def q_duplicated_albums (spark):
    sql_query = """
    WITH UserTriplets AS (
        SELECT
            MAX(CASE WHEN rank = 1 THEN album_id END) as album_id_1,
            MAX(CASE WHEN rank = 2 THEN album_id END) AS album_id_2,
            MAX(CASE WHEN rank = 3 THEN album_id END) AS album_id_3
        FROM
            fact_albums
        WHERE 
            RANK <=3
        GROUP BY
            user_id
    )

    SELECT album_id_1, album_id_2, album_id_3, COUNT(*) AS total_users, RANK() OVER(ORDER BY COUNT(*) DESC) AS Ranking_triple
    FROM
        UserTriplets
    GROUP BY
        album_id_1,
        album_id_2,
        album_id_3
    HAVING
        COUNT(*) > 1
    ORDER BY
        total_users DESC
    LIMIT 10;
    """
    return spark.sql(sql_query)

def q_duplicated_tracks(spark):
    sql_query = """
    WITH UserTriplets AS (
        SELECT
            user_id,
            MAX(CASE WHEN rank = 1 THEN track_id END) AS track_id_1,
            MAX(CASE WHEN rank = 2 THEN track_id END) AS track_id_2,
            MAX(CASE WHEN rank = 3 THEN track_id END) AS track_id_3
        FROM fact_tracks
        WHERE rank <= 3 AND track_id IS NOT NULL
        GROUP BY user_id
    )
    
    SELECT track_id_1, track_id_2, track_id_3,COUNT(*) AS total_users, RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_triple
    FROM UserTriplets
    GROUP BY 
        track_id_1, 
        track_id_2, 
        track_id_3
    HAVING 
        COUNT(*) > 1
    ORDER BY 
        total_users DESC
    LIMIT 10;
    """
    
    return spark.sql(sql_query)


#Question 10
def q_loyal_listeners(spark):

    sql_query = """
    WITH top5_tracks AS (
        SELECT 
            user_id, 
            artist_id
        FROM fact_tracks
        WHERE artist_id IS NOT NULL 
          AND rank <= 5
    ),
    
    loyal_users AS (
        SELECT
            user_id,
            MAX(artist_id) AS artist_id
        FROM top5_tracks
        GROUP BY user_id
        HAVING COUNT(DISTINCT artist_id) = 1 
           AND COUNT(*) = 5                   
    )
    
    SELECT
        artist_id,
        COUNT(*) AS loyal_user_count
    FROM loyal_users
    GROUP BY artist_id
    ORDER BY loyal_user_count DESC
    """
    return spark.sql(sql_query)



#Question 11
def q_paired_frequent_artists(spark):
    spark.sql("CACHE TABLE fact_artists")
    
    sql_query = """
    WITH user_artist_pairs AS (
        SELECT 
            a.user_id,
            LEAST(a.artist_id, b.artist_id) AS artist_1,
            GREATEST(a.artist_id, b.artist_id) AS artist_2
        FROM fact_artists a
        INNER JOIN fact_artists b 
            ON a.user_id = b.user_id 
            AND a.artist_id < b.artist_id
        WHERE a.artist_id IS NOT NULL 
          AND b.artist_id IS NOT NULL
    )
    
    SELECT 
        RANK() OVER(ORDER BY COUNT(DISTINCT user_id) DESC) AS ranking,
        artist_1,
        artist_2,
        COUNT(DISTINCT user_id) AS total_users
    FROM user_artist_pairs
    GROUP BY artist_1, artist_2
    ORDER BY ranking
    LIMIT 50
    """
    
    result = spark.sql(sql_query)
    
    spark.sql("UNCACHE TABLE fact_artists")
    
    return result

#Question 12
def q_triplet_artists(spark):
    from pyspark.sql.functions import collect_set, size, col, count as count_func
    from pyspark.sql.types import StructType, StructField, StringType, LongType
    from pyspark.sql import Window
    from pyspark.sql.functions import rank as rank_func
    from itertools import combinations
    from collections import Counter
    
    DW = "hdfs://namenode:9000/music_dw/"
    fact_artists = spark.read.parquet(DW + "fact_artists")
    
    top_artists = (
        fact_artists
        .filter(col("artist_id").isNotNull())
        .groupBy("artist_id")
        .agg(count_func("user_id").alias("total_users"))
        .orderBy(col("total_users").desc())
        .limit(100)
        .select("artist_id")
    )
    
    
    filtered_facts = (
        fact_artists
        .join(top_artists, "artist_id", "inner")
        .filter(col("artist_id").isNotNull())
    )
    
    user_artists = (
        filtered_facts
        .groupBy("user_id")
        .agg(collect_set("artist_id").alias("artists"))
        .filter(size("artists") >= 3)
    )
        
    def process_partition(partition):
        trio_counter = Counter()
        
        for row in partition:
            artists = sorted(set(row.artists))
            if len(artists) >= 3:
                for trio in combinations(artists, 3):
                    trio_counter[trio] += 1
        
        for trio, count in trio_counter.items():
            yield (trio[0], trio[1], trio[2], count)
    
    schema = StructType([
        StructField("artist_1", StringType(), False),
        StructField("artist_2", StringType(), False),
        StructField("artist_3", StringType(), False),
        StructField("count", LongType(), False)
    ])
    
    result = (
        user_artists
        .rdd
        .mapPartitions(process_partition)
        .toDF(schema)
        .groupBy("artist_1", "artist_2", "artist_3")
        .sum("count")
        .withColumnRenamed("sum(count)", "total_users") 
        .withColumn("ranking", rank_func().over(Window.orderBy(col("total_users").desc())))
        .select("ranking", "artist_1", "artist_2", "artist_3", "total_users") 
        .orderBy("ranking")
        .limit(20)  
    )
    
    return result

def main():
    spark = init_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    load_tables(spark)

    """ 
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
    print("Items_Per_User")


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
    print("Unique_Items")


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
    print("top_10_Duplicated_Artists")


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
    print("top_10_Duplicated_Albums")


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
    print("top_10_Duplicated_Songs")


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
    print("Loyal_Listeners")


    # =========================================================
    # ============= QUERY 11 (Pairs) ==========================
    # =========================================================
    print("\n=== QUERY 11: Paired Artists ===")
    df11 = load_parquet("top_artist_pairs") \
        .select(
            F.col("ranking").cast("int"),
            F.col("artist_1").alias("artist_1_id"),
            F.col("artist_2").alias("artist_2_id"),
            F.col("total_users").cast("int")
        )

    df11.write.mode("overwrite").jdbc(mysql_url, "top_50_Paired_Artists", properties=mysql_props)
    print("top_50_Paired_Artists")


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
    print("top_20_Trio_Artists")

    """
   
    df_metrics = q_avg_med_std(spark)
    df_metrics.show()

    row = df_metrics.collect()[0]

    insert_metrics(spark, query=7, average=row.average, median=row.median,standard_deviation=row.standard_deviation)

    df_loyal_count = q_loyal_listeners(spark)

    total_loyal = df_loyal_count.agg(
        F.sum("loyal_user_count").alias("total")
    ).collect()[0]["total"]

    insert_single_value (spark, query=10, value=total_loyal)

    print("Total loyal listeners:", total_loyal)


    spark.stop()



if __name__ == "__main__":
    main()


