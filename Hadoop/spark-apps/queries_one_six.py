from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def init_spark_session():
    return SparkSession.builder \
        .appName("Music DW Queries 1-6") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def load_tables(spark):
    DW = "hdfs://namenode:9000/music_dw/"

    dim_user = spark.read.parquet(DW + "user")
    dim_artist = spark.read.parquet(DW + "artist")
    dim_track = spark.read.parquet(DW + "track")
    dim_album = spark.read.parquet(DW + "album")

    fact_artists = spark.read.parquet(DW + "fact_artists")
    fact_tracks = spark.read.parquet(DW + "fact_tracks")
    fact_albums = spark.read.parquet(DW + "fact_albums")

    dim_user.createOrReplaceTempView("dim_user")
    dim_artist.createOrReplaceTempView("dim_artist")
    dim_track.createOrReplaceTempView("dim_track")
    dim_album.createOrReplaceTempView("dim_album")

    fact_artists.createOrReplaceTempView("fact_artists")
    fact_tracks.createOrReplaceTempView("fact_tracks")
    fact_albums.createOrReplaceTempView("fact_albums")

def q1_top_20_general_artists(spark):
    sql_query = """
    WITH artist_listeners AS (
        SELECT 
            artist_id,
            COUNT(DISTINCT user_id) as listeners,
            SUM(playcount_artist) as total_playcount
        FROM fact_artists 
        WHERE artist_id IS NOT NULL
        GROUP BY artist_id
    ),
    total_plays AS (
        SELECT SUM(total_playcount) as global_total
        FROM artist_listeners
    )
    SELECT 
        RANK() OVER (ORDER BY al.listeners DESC, al.total_playcount DESC) as ranking,
        al.artist_id,
        al.listeners,
        (al.total_playcount / tp.global_total) * 100 as participation_percentage
    FROM artist_listeners al
    CROSS JOIN total_plays tp
    ORDER BY ranking
    LIMIT 20
    """
    return spark.sql(sql_query)

def q2_top_20_general_songs(spark):
    sql_query = """
    WITH song_listeners AS (
        SELECT 
            track_id as song_id,
            COUNT(DISTINCT user_id) as listeners,
            SUM(playcount_track) as total_playcount
        FROM fact_tracks 
        WHERE track_id IS NOT NULL
        GROUP BY track_id
    ),
    total_plays AS (
        SELECT SUM(total_playcount) as global_total
        FROM song_listeners
    )
    SELECT 
        RANK() OVER (ORDER BY sl.listeners DESC, sl.total_playcount DESC) as ranking,
        sl.song_id,
        sl.listeners,
        (sl.total_playcount / tp.global_total) * 100 as participation_percentage
    FROM song_listeners sl
    CROSS JOIN total_plays tp
    ORDER BY ranking
    LIMIT 20
    """
    return spark.sql(sql_query)

def q3_top_20_general_albums(spark):
    sql_query = """
    WITH album_listeners AS (
        SELECT 
            album_id,
            COUNT(DISTINCT user_id) as listeners,
            SUM(playcount_album) as total_playcount
        FROM fact_albums 
        WHERE album_id IS NOT NULL
        GROUP BY album_id
    ),
    total_plays AS (
        SELECT SUM(total_playcount) as global_total
        FROM album_listeners
    )
    SELECT 
        RANK() OVER (ORDER BY al.listeners DESC, al.total_playcount DESC) as ranking,
        al.album_id,
        al.listeners,
        (al.total_playcount / tp.global_total) * 100 as participation_percentage
    FROM album_listeners al
    CROSS JOIN total_plays tp
    ORDER BY ranking
    LIMIT 20
    """
    return spark.sql(sql_query)

def q4_same_top_one_artist(spark):
    sql_query = """
    WITH top_artists AS (
        SELECT 
            user_id,
            artist_id
        FROM fact_artists
        WHERE rank = 1 AND artist_id IS NOT NULL
    )
    SELECT 
        artist_id,
        COUNT(*) as frequency
    FROM top_artists
    GROUP BY artist_id
    ORDER BY frequency DESC
    """
    return spark.sql(sql_query)

def q5_mentions_per_artist(spark):
    sql_query = """
    WITH artist_mentions AS (
        SELECT 
            artist_id,
            COUNT(*) as mentions
        FROM fact_artists
        WHERE artist_id IS NOT NULL
        GROUP BY artist_id
    )
    SELECT 
        artist_id,
        mentions
    FROM artist_mentions
    ORDER BY mentions DESC
    """
    return spark.sql(sql_query)

def q5_calculate_metrics(spark, df_q5):
    # More accurate median calculation using window functions
    window_spec = Window.orderBy("mentions")
    
    median_df = df_q5.withColumn("row_num", F.row_number().over(window_spec)) \
                    .withColumn("total_count", F.count("*").over(Window.partitionBy()))
    
    total_count = df_q5.count()
    
    if total_count % 2 == 0:
        # Even number of elements - average of two middle values
        median_val = median_df.filter(
            (F.col("row_num") == total_count // 2) | (F.col("row_num") == total_count // 2 + 1)
        ).agg(F.avg("mentions")).collect()[0][0]
    else:
        # Odd number of elements - middle value
        median_val = median_df.filter(F.col("row_num") == (total_count + 1) // 2) \
                            .select("mentions").collect()[0]["mentions"]
    
    metrics = df_q5.agg(
        F.avg("mentions").alias("average"),
        F.stddev("mentions").alias("standard_deviation")
    ).collect()[0]
    
    return {
        "query": 5,
        "average": metrics["average"],
        "median": float(median_val),
        "standard_deviation": metrics["standard_deviation"]
    }

def q6_long_tail(spark):
    sql_query = """
    WITH artist_mentions AS (
        SELECT 
            artist_id,
            COUNT(*) as mentions
        FROM fact_artists
        WHERE artist_id IS NOT NULL
        GROUP BY artist_id
    ),
    total_mentions AS (
        SELECT SUM(mentions) as total
        FROM artist_mentions
    ),
    cumulative_mentions AS (
        SELECT 
            artist_id,
            mentions,
            SUM(mentions) OVER (ORDER BY mentions DESC) as running_total,
            (SUM(mentions) OVER (ORDER BY mentions DESC)) / (SELECT total FROM total_mentions) as cumulative_percentage
        FROM artist_mentions
    )
    SELECT 
        artist_id,
        cumulative_percentage * 100 as mentions_percentage
    FROM cumulative_mentions
    WHERE cumulative_percentage <= 0.8
    ORDER BY cumulative_percentage
    """
    return spark.sql(sql_query)

def q6_calculate_single_value(spark, df_q6):
    total_artists_query = """
    SELECT COUNT(DISTINCT artist_id) as total_artists
    FROM fact_artists 
    WHERE artist_id IS NOT NULL
    """
    total_artists = spark.sql(total_artists_query).collect()[0]["total_artists"]
    
    long_tail_artists = df_q6.count()
    
    percentage = (long_tail_artists / total_artists) * 100 if total_artists > 0 else 0
    
    return {
        "query": 6,
        "value": percentage
    }

def main():
    spark = init_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    load_tables(spark)
    
    print("Running Query 1: Top 20 General Artists")
    q1_top_20_general_artists(spark).write.mode("overwrite").parquet("/opt/spark-apps/music_results/q1_top_artists")
    
    print("Running Query 2: Top 20 General Songs")
    q2_top_20_general_songs(spark).write.mode("overwrite").parquet("/opt/spark-apps/music_results/q2_top_songs")
    
    print("Running Query 3: Top 20 General Albums")
    q3_top_20_general_albums(spark).write.mode("overwrite").parquet("/opt/spark-apps/music_results/q3_top_albums")
    
    print("Running Query 4: Same Top One Artist")
    q4_same_top_one_artist(spark).write.mode("overwrite").parquet("/opt/spark-apps/music_results/q4_same_top_artist")
    
    print("Running Query 5: Mentions Per Artist")
    df_q5 = q5_mentions_per_artist(spark)
    df_q5.write.mode("overwrite").parquet("/opt/spark-apps/music_results/q5_mentions_artist")
    metrics_q5 = q5_calculate_metrics(spark, df_q5)
    
    print("Running Query 6: Long Tail")
    df_q6 = q6_long_tail(spark)
    df_q6.write.mode("overwrite").parquet("/opt/spark-apps/music_results/q6_long_tail")
    single_value_q6 = q6_calculate_single_value(spark, df_q6)
    
    metrics_data = [metrics_q5]
    single_value_data = [single_value_q6]
    
    metrics_df = spark.createDataFrame(metrics_data)
    single_value_df = spark.createDataFrame(single_value_data)
    
    metrics_df.write.mode("overwrite").parquet("/opt/spark-apps/music_results/q_metrics")
    single_value_df.write.mode("overwrite").parquet("/opt/spark-apps/music_results/q_single_values")
    
    print("✅ All queries 1-6 completed and saved as Parquet files")
    
    spark.stop()

if __name__ == "__main__":
    main()