from metrics_inserts import init_spark_session


def load_tables(spark):
    DW = "hdfs://namenode:9000/music_dw/"

    dim_artist = spark.read.parquet(DW + "artist")
    dim_track  = spark.read.parquet(DW + "track")
    dim_album  = spark.read.parquet(DW + "album")

    fact_artists = spark.read.parquet(DW + "fact_artists")
    fact_tracks  = spark.read.parquet(DW + "fact_tracks")
    fact_albums  = spark.read.parquet(DW + "fact_albums")

    # Crear vistas SQL
    dim_artist.createOrReplaceTempView("dim_artist")
    dim_track.createOrReplaceTempView("dim_track")
    dim_album.createOrReplaceTempView("dim_album")

    fact_artists.createOrReplaceTempView("fact_artists")
    fact_tracks.createOrReplaceTempView("fact_tracks")
    fact_albums.createOrReplaceTempView("fact_albums")

#Question 7
def q_items_usuar(spark):
    sql_query = """
    SELECT user_id,
        artists + tracks + albums AS total_items
    FROM (
        SELECT user_id,
               COUNT(DISTINCT artist_id) AS artists
        FROM fact_artists
        WHERE artist_id IS NOT NULL
        GROUP BY user_id
    ) a
    FULL OUTER JOIN (
        SELECT user_id,
               COUNT(DISTINCT track_id) AS tracks
        FROM fact_tracks
        WHERE track_id IS NOT NULL
        GROUP BY user_id
    ) t USING (user_id)
    FULL OUTER JOIN (
        SELECT user_id,
               COUNT(DISTINCT album_id) AS albums
        FROM fact_albums
        WHERE album_id IS NOT NULL
        GROUP BY user_id
    ) al USING (user_id)
    """
    return spark.sql(sql_query)

def q_avg_med_std(spark):
    sql_query = """
    WITH items AS (

    SELECT user_id,
        artists + tracks + albums AS total_items
        FROM (
            SELECT user_id,
                COUNT(DISTINCT artist_id) AS artists
            FROM fact_artists
            WHERE artist_id IS NOT NULL
            GROUP BY user_id
        ) a
        FULL OUTER JOIN (
            SELECT user_id,
                COUNT(DISTINCT track_id) AS tracks
            FROM fact_tracks
            WHERE track_id IS NOT NULL
            GROUP BY user_id
        ) t USING (user_id)
        FULL OUTER JOIN (
            SELECT user_id,
                COUNT(DISTINCT album_id) AS albums
            FROM fact_albums
            WHERE album_id IS NOT NULL
            GROUP BY user_id
        ) al USING (user_id)
    )
    
    SELECT AVG(total_items) as average, percentile_approx(total_items, 0.5) AS median,
    stddev(total_items) AS standard_deviation
    FROM items
    """

    return spark.sql(sql_query)

#Question 10
def q_loyal_listeners(spark):
    sql_query = """
    WITH unified AS (
        SELECT user_id, rank, artist_id
        FROM fact_artists
        WHERE artist_id IS NOT NULL AND rank <= 5

        UNION ALL

        SELECT user_id, rank, artist_id
        FROM fact_tracks
        WHERE artist_id IS NOT NULL AND rank <= 5

        UNION ALL

        SELECT user_id, rank, artist_id
        FROM fact_albums
        WHERE artist_id IS NOT NULL AND rank <= 5
    ),

    ranked5 AS (
        SELECT
            user_id,
            artist_id,
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY rank) AS rn
        FROM unified
    ),

    top5 AS (
        SELECT user_id, artist_id
        FROM ranked5
        WHERE rn <= 5
    ),

    loyal_users AS (
        SELECT
            user_id,
            MAX(artist_id) AS artist_id
        FROM top5
        GROUP BY user_id
        HAVING COUNT(DISTINCT artist_id) = 1
    )

    SELECT
        artist_id,
        COUNT(*) AS loyal_user_count
    FROM loyal_users
    GROUP BY artist_id
    ORDER BY loyal_user_count DESC
    """
    return spark.sql(sql_query)

#Pregunta 12
def q_trio_artists(spark):
    sql_query = """
    WITH user_artists AS (
        -- Lista completa de artistas por usuario
        SELECT
            user_id,
            artist_id
        FROM fact_artists
        WHERE artist_id IS NOT NULL
        GROUP BY user_id, artist_id
    ),

    artist_pairs AS (
        -- Generamos todas las combinaciones de 3 artistas por usuario
        -- con combinaciones internas ordenadas para evitar duplicados
        SELECT
            a.user_id,
            LEAST(a.artist_id, b.artist_id, c.artist_id) AS artist_1,
            (
                a.artist_id + b.artist_id + c.artist_id
                - LEAST(a.artist_id, b.artist_id, c.artist_id)
                - GREATEST(a.artist_id, b.artist_id, c.artist_id)
            ) AS artist_2,
            GREATEST(a.artist_id, b.artist_id, c.artist_id) AS artist_3
        FROM user_artists a
        JOIN user_artists b
          ON a.user_id = b.user_id AND a.artist_id < b.artist_id
        JOIN user_artists c
          ON a.user_id = c.user_id AND b.artist_id < c.artist_id
    )

    SELECT
        artist_1,
        artist_2,
        artist_3,
        COUNT(*) AS trio_count
    FROM artist_pairs
    GROUP BY artist_1, artist_2, artist_3
    ORDER BY trio_count DESC
    LIMIT 20
    """
    return spark.sql(sql_query)


def main():
    spark = init_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    load_tables(spark)

    print("===Items por usuario===")
    df = q_items_usuar(spark)
    df.show(50, truncate=False)

    print("===Promedios===")
    dp=q_avg_med_std(spark)
    dp.show(50, truncate=False)

    print("===Fieles===")
    dp=q_loyal_listeners(spark)
    dp.show(50, truncate=False)

    print("===TrioArtistas===")
    dp=q_trio_artists(spark)
    dp.show(10, truncate=False)

if __name__ == "__main__":
    main()


