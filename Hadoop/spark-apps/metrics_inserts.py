from pyspark.sql import SparkSession

# Initialize Spark session
def init_spark_session():
    spark = SparkSession.builder \
        .appName("Metrics Inserts") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    return spark

# MySQL connection settings
mysql_url = "jdbc:mysql://host.docker.internal:3307/metrics"
mysql_props = {
    "user": "root",
    "password": "BDII_PROYII",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Metrics
def insert_metrics(spark, query, average, median, standard_deviation):
    df = spark.createDataFrame(
        [(query, average, median, standard_deviation)],
        ["query", "average", "median", "standard_deviation"]
    )
    df.write.jdbc(mysql_url, "Metrics", "append", mysql_props)

# Single_Value_Queries
def insert_single_value(spark, query, value):
    df = spark.createDataFrame(
        [(query, value)],
        ["query", "value"]
    )
    df.write.jdbc(mysql_url, "Single_Value_Queries", "append", mysql_props)

# 1. Top 20 artistas en general — contar cuántos usuarios incluyen cada artista; 
# mostrar el top 20 y su porcentaje de participación en el total de las reproducciones.
def insert_top_general_artist(spark, ranking, artist_id, listeners, participation_percentage):
    df = spark.createDataFrame(
        [(ranking, artist_id, listeners, participation_percentage)],
        ["ranking", "artist_id", "listeners", "participation_percentage"]
    )
    df.write.jdbc(mysql_url, "Top_20_General_Artists", "append", mysql_props)

# 2. Top 20 canciones en general — igual que los artistas, solo que para canciones.
def insert_top_general_song(spark, ranking, song_id, listeners, participation_percentage):
    df = spark.createDataFrame(
        [(ranking, song_id, listeners, participation_percentage)],
        ["ranking", "song_id", "listeners", "participation_percentage"]
    )
    df.write.jdbc(mysql_url, "Top_20_General_Songs", "append", mysql_props)

# 3. Top 20 álbumes en general — igual que los artistas, solo que para álbumes.
def insert_top_general_album(spark, ranking, album_id, listeners, participation_percentage):
    df = spark.createDataFrame(
        [(ranking, album_id, listeners, participation_percentage)],
        ["ranking", "album_id", "listeners", "participation_percentage"]
    )
    df.write.jdbc(mysql_url, "Top_20_General_Albums", "append", mysql_props)

# 4. Cuántos usuarios comparten el mismo artista #1 — 
# contar usuarios por su artista principal y reportar la moda y su frecuencia.
def insert_same_top_one_artist(spark, artist_id, frequency):
    df = spark.createDataFrame(
        [(artist_id, frequency)],
        ["artist_id", "frequency"]
    )
    df.write.jdbc(mysql_url, "Same_Top_One_Artist", "append", mysql_props)

# 5. Distribución de menciones por artista — 
# histograma de veces que aparece cada artista; reportar media, mediana, desviación estándar.
def insert_mentions_per_artist(spark, artist_id, mentions):
    df = spark.createDataFrame(
        [(artist_id, mentions)],
        ["artist_id", "mentions"]
    )
    df.write.jdbc(mysql_url, "Metions_Per_Artist", "append", mysql_props)

# 6. Participación del long tail — 
# calcular qué porcentaje de artistas acumula el 80% de las menciones.
def insert_long_tail(spark, artist_id, mentions_percentage):
    df = spark.createDataFrame(
        [(artist_id, mentions_percentage)],
        ["artist_id", "mentions_percentage"]
    )
    df.write.jdbc(mysql_url, "Long_Tail", "append", mysql_props)

# 7. Ítems por usuario — contar cuántos artistas/canciones/álbumes 
# lista cada usuario; reportar media y mediana.
def insert_items_per_user(spark, user_id, items):
    df = spark.createDataFrame(
        [(user_id, items)],
        ["user_id", "items"]
    )
    df.write.jdbc(mysql_url, "Items_Per_User", "append", mysql_props)

# 8. Artistas únicos en el conjunto — número total de artistas, canciones y álbumes distintos.
def insert_unique_items(spark, user_id, artists, songs, albums):
    df = spark.createDataFrame(
        [(user_id, artists, songs, albums)],
        ["user_id", "artists", "songs", "albums"]
    )
    df.write.jdbc(mysql_url, "Unique_Items", "append", mysql_props)

# 9. Usuarios con listas top-3 idénticas — contar duplicados de top-10 y mostrar las duplicaciones más comunes.
def insert_top10_duplicated_artist(spark, ranking, artist_id_1, artist_id_2, artist_id_3, total_users):
    df = spark.createDataFrame(
        [(ranking, artist_id_1, artist_id_2, artist_id_3, total_users)],
        ["ranking", "artist_id_1", "artist_id_2", "artist_id_3", "total_users"]
    )
    df.write.jdbc(mysql_url, "top_10_Duplicated_Artists", "append", mysql_props)


def insert_top10_duplicated_album(spark, ranking, album_id_1, album_id_2, album_id_3, total_users):
    df = spark.createDataFrame(
        [(ranking, album_id_1, album_id_2, album_id_3, total_users)],
        ["ranking", "album_id_1", "album_id_2", "album_id_3", "total_users"]
    )
    df.write.jdbc(mysql_url, "top_10_Duplicated_Albums", "append", mysql_props)


def insert_top10_duplicated_song(spark, ranking, song_id_1, song_id_2, song_id_3, total_users):
    df = spark.createDataFrame(
        [(ranking, song_id_1, song_id_2, song_id_3, total_users)],
        ["ranking", "song_id_1", "song_id_2", "song_id_3", "total_users"]
    )
    df.write.jdbc(mysql_url, "top_10_Duplicated_Songs", "append", mysql_props)


# 10. Usuarios con gustos muy concentrados — contar usuarios cuyo top 5 pertenece todo al mismo
# artista.
def insert_loyal_listeners(spark, artist_id, loyal_listeners):
    df = spark.createDataFrame(
        [(artist_id, loyal_listeners)],
        ["artist_id", "loyal_listeners"]
    )
    df.write.jdbc(mysql_url, "Loyal_Listeners", "append", mysql_props)

# 11. Pares de artistas más frecuentes — contar cuántas veces dos artistas aparecen juntos en la
# misma lista de usuario; mostrar top 50 pares.
def insert_paired_artists(spark, ranking, artist_1_id, artist_2_id):
    df = spark.createDataFrame(
        [(ranking, artist_1_id, artist_2_id)],
        ["ranking", "artist_1_id", "artist_2_id"]
    )
    df.write.jdbc(mysql_url, "top_50_Paired_Artists", "append", mysql_props)

# 12. Combinaciones de 3 artistas frecuentes — contar tripletas frecuentes.
def insert_trio_artists(spark, ranking, artist_1_id, artist_2_id, artist_3_id):
    df = spark.createDataFrame(
        [(ranking, artist_1_id, artist_2_id, artist_3_id)],
        ["ranking", "artist_1_id", "artist_2_id", "artist_3_id"]
    )
    df.write.jdbc(mysql_url, "top_20_Trio_Artists", "append", mysql_props)

# 14. Posición promedio por artista — para cada artista, calcular 
# la posición media en las listas de los usuarios que lo incluyen.
def insert_average_artist_position(spark, artist_id, average):
    df = spark.createDataFrame(
        [(artist_id, average)],
        ["artist_id", "average"]
    )
    df.write.jdbc(mysql_url, "Average_Artist_Position", "append", mysql_props)

# 15. Frecuencia de que el #1 esté en el top 5 global — 
# proporción de usuarios cuyo artista #1 también figura entre los 5 más populares globales.
def insert_global_top5_correlation(spark, ranking, artist_id, users):
    df = spark.createDataFrame(
        [(ranking, artist_id, users)],
        ["ranking", "artist_id", "users"]
    )
    df.write.jdbc(mysql_url, "Global_Top_5_Correlates_Top_Per_User", "append", mysql_props)

# 16. Estabilidad de posiciones — contar usuarios que tienen el mismo artista en las posiciones #1 y #2.
def insert_same_top_1_2(spark, id_artist_position_1, id_artist_position_2, users):
    df = spark.createDataFrame(
        [(id_artist_position_1, id_artist_position_2, users)],
        ["id_artist_position_1", "id_artist_position_2", "users"]
    )
    df.write.jdbc(mysql_url, "Same_Top_1_And_2", "append", mysql_props)

# 18. Top artistas entre oyentes— definir oyentes que tienen más de 40 canciones y contar sus
# artistas principales.
def insert_top_artists_between_listeners(spark, ranking, artist_id):
    df = spark.createDataFrame(
        [(ranking, artist_id)],
        ["ranking", "artist_id"]
    )
    df.write.jdbc(mysql_url, "Top_Artists_In_Between_Listeners", "append", mysql_props)

# 19. Popularidad cruzada entre listas — contar artistas que aparecen con más frecuencia en la lista
# de canciones vs. la de artistas; reportar la diferencia de conteos.
def insert_cross_popularity(spark, artist_id, song_frequency, artist_frequency, difference):
    df = spark.createDataFrame(
        [(artist_id, song_frequency, artist_frequency, difference)],
        ["artist_id", "song_frequency", "artist_frequency", "difference"]
    )
    df.write.jdbc(mysql_url, "Cross_Popularity", "append", mysql_props)

# 20. Artistas más diversos — contar cuántos usuarios distintos listan cada artista y cuántas canciones
# distintas tiene cada artista en el conjunto.
def insert_diverse_artist(spark, artist_id, listeners, songs):
    df = spark.createDataFrame(
        [(artist_id, listeners, songs)],
        ["artist_id", "listeners", "songs"]
    )
    df.write.jdbc(mysql_url, "Diverse_Artists", "append", mysql_props)

# 23. Artistas de baja cobertura — contar cuántos artistas aparecen menos de 5 veces.
def insert_low_coverage_artist(spark, artist_id, appearances):
    df = spark.createDataFrame(
        [(artist_id, appearances)],
        ["artist_id", "appearances"]
    )
    df.write.jdbc(mysql_url, "Low_Coverage_Artists", "append", mysql_props)

