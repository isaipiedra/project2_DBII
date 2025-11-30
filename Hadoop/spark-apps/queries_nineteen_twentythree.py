from metrics_inserts import init_spark_session
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from metrics_inserts import (
    init_spark_session,

)

def load_tables(spark):
    """Carga las tablas del Data Warehouse y crea vistas SQL"""
    DW = "hdfs://namenode:9000/music_dw/"

    dim_user = spark.read.parquet(DW + "user")
    dim_artist = spark.read.parquet(DW + "artist")
    dim_track = spark.read.parquet(DW + "track")
    dim_album = spark.read.parquet(DW + "album")

    fact_artists = spark.read.parquet(DW + "fact_artists")
    fact_tracks = spark.read.parquet(DW + "fact_tracks")
    fact_albums = spark.read.parquet(DW + "fact_albums")

    # Crear vistas SQL
    dim_user.createOrReplaceTempView("dim_user")
    dim_artist.createOrReplaceTempView("dim_artist")
    dim_track.createOrReplaceTempView("dim_track")
    dim_album.createOrReplaceTempView("dim_album")

    fact_artists.createOrReplaceTempView("fact_artists")
    fact_tracks.createOrReplaceTempView("fact_tracks")
    fact_albums.createOrReplaceTempView("fact_albums")


# Question 19: Popularidad cruzada entre listas
def q_cross_popularity(spark):
    """
    Contar artistas que aparecen con más frecuencia en la lista de canciones 
    vs. la de artistas; reportar la diferencia de conteos.
    song_frequency: Por cada artista, contar todas las apariciones de sus canciones
    artist_frequency: Por cada artista, contar todas sus apariciones en fact_artists
    """
    sql_query = """
    WITH song_frequency AS (
        SELECT 
            artist_id,
            COUNT(*) AS song_frequency
        FROM fact_tracks
        WHERE artist_id IS NOT NULL
        GROUP BY artist_id
    ),
    artist_frequency AS (
        SELECT 
            artist_id,
            COUNT(*) AS artist_frequency
        FROM fact_artists
        WHERE artist_id IS NOT NULL
        GROUP BY artist_id
    )
    
    SELECT 
        COALESCE(s.artist_id, a.artist_id) AS artist_id,
        COALESCE(s.song_frequency, 0) AS song_frequency,
        COALESCE(a.artist_frequency, 0) AS artist_frequency,
        ABS(COALESCE(s.song_frequency, 0) - COALESCE(a.artist_frequency, 0)) AS difference
    FROM song_frequency s
    FULL OUTER JOIN artist_frequency a ON s.artist_id = a.artist_id
    ORDER BY difference DESC
    """
    return spark.sql(sql_query)


# Question 23: Artistas de baja cobertura
def q_low_coverage_artists(spark):
    """
    Contar cuántos artistas aparecen menos de 5 veces.
    Ahora cuenta todas las apariciones
    """
    sql_query = """
    WITH artist_appearances AS (
        SELECT 
            artist_id,
            COUNT(*) AS appearances
        FROM fact_artists
        WHERE artist_id IS NOT NULL
        GROUP BY artist_id
    )
    
    SELECT 
        artist_id,
        appearances
    FROM artist_appearances
    WHERE appearances < 5
    ORDER BY appearances ASC, artist_id
    """
    return spark.sql(sql_query)


# Question 20: Artistas más diversos
def q_diverse_artists(spark):
    """
    Contar cuántos usuarios distintos listan cada artista y cuántas canciones 
    distintas tiene cada artista en el conjunto.
    """
    sql_query = """
    WITH artist_listeners AS (
        SELECT 
            artist_id,
            COUNT(DISTINCT user_id) AS listeners
        FROM fact_artists
        WHERE artist_id IS NOT NULL
        GROUP BY artist_id
    ),
    artist_songs AS (
        SELECT 
            artist_id,
            COUNT(DISTINCT track_id) AS songs
        FROM fact_tracks
        WHERE artist_id IS NOT NULL AND track_id IS NOT NULL
        GROUP BY artist_id
    )
    
    SELECT 
        COALESCE(l.artist_id, s.artist_id) AS artist_id,
        COALESCE(l.listeners, 0) AS listeners,
        COALESCE(s.songs, 0) AS songs
    FROM artist_listeners l
    FULL OUTER JOIN artist_songs s ON l.artist_id = s.artist_id
    ORDER BY listeners DESC, songs DESC
    """
    return spark.sql(sql_query)


# Question 21: Conteo de datos faltantes
def q_missing_data_count(spark):

    sql_query = """
    WITH artist_counts AS (
        SELECT 
            user_id,
            COUNT(DISTINCT artist_id) AS artist_count
        FROM fact_artists
        WHERE artist_id IS NOT NULL
        GROUP BY user_id
    ),
    track_counts AS (
        SELECT 
            user_id,
            COUNT(DISTINCT track_id) AS track_count
        FROM fact_tracks
        WHERE track_id IS NOT NULL
        GROUP BY user_id
    ),
    album_counts AS (
        SELECT 
            user_id,
            COUNT(DISTINCT album_id) AS album_count
        FROM fact_albums
        WHERE album_id IS NOT NULL
        GROUP BY user_id
    ),
    user_items AS (
        SELECT 
            u.user_id,
            u.country,
            u.total_scrobbles,
            COALESCE(a.artist_count, 0) AS artist_count,
            COALESCE(t.track_count, 0) AS track_count,
            COALESCE(al.album_count, 0) AS album_count
        FROM dim_user u
        LEFT JOIN artist_counts a ON u.user_id = a.user_id
        LEFT JOIN track_counts t ON u.user_id = t.user_id
        LEFT JOIN album_counts al ON u.user_id = al.user_id
    )
    
    SELECT 
        COUNT(*) AS total_users_with_issues
    FROM user_items
    WHERE country IS NULL 
       OR total_scrobbles IS NULL 
       OR artist_count = 0 
       OR track_count = 0 
       OR album_count = 0
       OR artist_count < 50
       OR track_count < 50
       OR album_count < 50
    """
    return spark.sql(sql_query)

# Question 22: Usuarios atípicos
def q_outlier_users_count(spark):
    """
    Contar usuarios con recuentos de ítems extremadamente altos o bajos, 
    que están en el percentil 99.
    """
    sql_query = """
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
    ),
    
    total_items AS (
        SELECT 
            user_id,
            SUM(item_count) AS total_items
        FROM user_counts
        GROUP BY user_id
    ),
    
    percentiles AS (
        SELECT 
            percentile_approx(total_items, 0.01) AS p01,
            percentile_approx(total_items, 0.99) AS p99
        FROM total_items
    )
    
    SELECT 
        COUNT(*) AS outlier_count
    FROM total_items t
    CROSS JOIN percentiles p
    WHERE t.total_items <= p.p01 OR t.total_items >= p.p99
    """
    return spark.sql(sql_query)





def main():
    spark = init_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Cargar tablas
    load_tables(spark)
    
    BASE_OUTPUT = "/opt/spark-apps/music_results"
    
    # Query 19: Cross Popularity
    print("\n=== Ejecutando Query 19: Cross Popularity ===")
    df19 = q_cross_popularity(spark)
    df19.write.mode("overwrite").parquet(f"{BASE_OUTPUT}/q19_cross_popularity")
    print(f"✅ Guardado en Parquet: q19_cross_popularity")
    
    # Query 20: Diverse Artists
    print("\n=== Ejecutando Query 20: Diverse Artists ===")
    df20 = q_diverse_artists(spark)
    df20.write.mode("overwrite").parquet(f"{BASE_OUTPUT}/q20_diverse_artists")
    print(f"✅ Guardado en Parquet: q20_diverse_artists")
    
    # Query 21: Missing Data Count
    print("\n=== Ejecutando Query 21: Missing Data Count ===")
    df21 = q_missing_data_count(spark)
    df21.write.mode("overwrite").parquet(f"{BASE_OUTPUT}/q21_missing_data_count")
    print(f"✅ Guardado en Parquet: q21_missing_data_count")
    
    # Query 22: Outlier Users Count
    print("\n=== Ejecutando Query 22: Outlier Users Count ===")
    df22 = q_outlier_users_count(spark)
    df22.write.mode("overwrite").parquet(f"{BASE_OUTPUT}/q22_outlier_users_count")
    print(f"✅ Guardado en Parquet: q22_outlier_users_count")
    
    # Query 23: Low Coverage Artists
    print("\n=== Ejecutando Query 23: Low Coverage Artists ===")
    df23 = q_low_coverage_artists(spark)
    df23.write.mode("overwrite").parquet(f"{BASE_OUTPUT}/q23_low_coverage_artists")
    print(f"✅ Guardado en Parquet: q23_low_coverage_artists")

    
    print("\nFinalizado\n")
    
    spark.stop()


if __name__ == "__main__":
    main()