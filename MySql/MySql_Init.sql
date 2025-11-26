CREATE DATABASE IF NOT EXISTS metrics;
USE metrics;

CREATE TABLE IF NOT EXISTS Artists (
	id INT PRIMARY KEY,
    name VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS Albums (
	id INT PRIMARY KEY,
    name VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS Songs (
	id INT PRIMARY KEY,
    name VARCHAR(500)
);

/*Metricas de las consultas que lo necesiten*/
CREATE TABLE IF NOT EXISTS Metrics(
	query INT PRIMARY KEY,
    average DOUBLE,
    median DOUBLE,
    standard_deviation DOUBLE
);

/*Tabla para las consultas que solo devuelven un valor*/
CREATE TABLE IF NOT EXISTS Single_Value_Queries(
	query INT PRIMARY KEY,
    value DOUBLE
);

/*1. Top 20 artistas en general — contar cuántos usuarios incluyen cada artista; 
mostrar el top 20 y su porcentaje de participación en el total de las reproducciones.*/

CREATE TABLE IF NOT EXISTS Top_20_General_Artists(
	ranking INT PRIMARY KEY,
    artist_id INT,
    FOREIGN KEY (artist_id) REFERENCES Artists(id),
    listeners INT,
    participation_percentage DOUBLE
);

/*2. Top 20 canciones en general — igual que los artistas, solo que para canciones.*/
CREATE TABLE IF NOT EXISTS Top_20_General_Songs(
	ranking INT PRIMARY KEY,
    song_id INT,
    FOREIGN KEY (song_id) REFERENCES Songs(id),
    listeners INT,
    participation_percentage DOUBLE
);

/*3. Top 20 álbumes en general — igual que los artistas, solo que para álbumes.*/
CREATE TABLE IF NOT EXISTS Top_20_General_Albums(
	ranking INT PRIMARY KEY,
    album_id INT,
    FOREIGN KEY (album_id) REFERENCES Albums(id),
    listeners INT,
    participation_percentage DOUBLE
);

/*4. Cuántos usuarios comparten el mismo artista #1 — 
contar usuarios por su artista principal y reportar la moda y su frecuencia.*/
CREATE TABLE IF NOT EXISTS Same_Top_One_Artist(
	artist_id INT PRIMARY KEY,
    FOREIGN KEY (artist_id) REFERENCES Artists(id),
    frequency INT
);

/*5. Distribución de menciones por artista — 
histograma de veces que aparece cada artista; reportar media, mediana, desviación estándar.*/
CREATE TABLE IF NOT EXISTS Metions_Per_Artist(
	artist_id INT PRIMARY KEY,
    FOREIGN KEY (artist_id) REFERENCES Artists(id),
    mentions INT
);

/*6. Participación del long tail — 
calcular qué porcentaje de artistas acumula el 80% de las menciones.*/
CREATE TABLE IF NOT EXISTS Long_Tail(
	artist_id INT PRIMARY KEY,
    FOREIGN KEY (artist_id) REFERENCES Artists(id),
    mentions_percentage DOUBLE
);

/*7. Ítems por usuario — contar cuántos artistas/canciones/álbumes 
lista cada usuario; reportar media y mediana.*/
CREATE TABLE IF NOT EXISTS Items_Per_User(
	user_id INT PRIMARY KEY,
    items INT
);

/*8. Artistas únicos en el conjunto — número total de artistas, canciones y álbumes distintos.*/
CREATE TABLE IF NOT EXISTS Unique_Items(
	user_id INT PRIMARY KEY,
    artists INT,
    songs INT,
    albums INT
);

/*9. Usuarios con listas top-3 idénticas — contar duplicados de top-10 y mostrar las duplicaciones más comunes.*/
CREATE TABLE IF NOT EXISTS top_10_Duplicated_Artists(
	ranking INT PRIMARY KEY,
	artist_id INT,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

CREATE TABLE IF NOT EXISTS top_10_Duplicated_Albums(
	ranking INT PRIMARY KEY,
	album_id INT,
    FOREIGN KEY (album_id) REFERENCES Albums(id)
);

CREATE TABLE IF NOT EXISTS top_10_Duplicated_Songs(
	ranking INT PRIMARY KEY,
	song_id INT,
    FOREIGN KEY (song_id) REFERENCES Songs(id)
);

/*10. Usuarios con gustos muy concentrados — contar usuarios cuyo top 5 pertenece todo al mismo
artista.*/
CREATE TABLE IF NOT EXISTS Loyal_Listeners(
	artist_id INT PRIMARY KEY,
    FOREIGN KEY (artist_id) REFERENCES Artists(id),
    loyal_listeners INT
);

/*11. Pares de artistas más frecuentes — contar cuántas veces dos artistas aparecen juntos en la
misma lista de usuario; mostrar top 50 pares.*/
CREATE TABLE IF NOT EXISTS top_50_Paired_Artists(
	ranking INT PRIMARY KEY,
	artist_1_id INT,
    FOREIGN KEY (artist_1_id) REFERENCES Artists(id),
	artist_2_id INT,
    FOREIGN KEY (artist_2_id) REFERENCES Artists(id)
);

/*12. Combinaciones de 3 artistas frecuentes — contar tripletas frecuentes.*/
CREATE TABLE IF NOT EXISTS top_20_Trio_Artists(
	ranking INT PRIMARY KEY,
	artist_1_id INT,
    FOREIGN KEY (artist_1_id) REFERENCES Artists(id),
	artist_2_id INT,
    FOREIGN KEY (artist_2_id) REFERENCES Artists(id),
	artist_3_id INT,
    FOREIGN KEY (artist_3_id) REFERENCES Artists(id)
);

/*14. Posición promedio por artista — para cada artista, calcular 
la posición media en las listas de los usuarios que lo incluyen.*/
CREATE TABLE IF NOT EXISTS Average_Artist_Position(
	artist_id INT PRIMARY KEY,
    FOREIGN KEY (artist_id) REFERENCES Artists(id),
    average DOUBLE
);

/*15. Frecuencia de que el #1 esté en el top 5 global — 
proporción de usuarios cuyo artista #1 también figura entre los 5 más populares globales.*/
CREATE TABLE IF NOT EXISTS Global_Top_5_Correlates_Top_Per_User(
	ranking INT PRIMARY KEY,
	artist_id INT,
    FOREIGN KEY (artist_id) REFERENCES Artists(id),
	users INT
);

/*16. Estabilidad de posiciones — contar usuarios que tienen el mismo artista en las posiciones #1 y #2.*/
CREATE TABLE IF NOT EXISTS Same_Top_1_And_2(
	id_artist_position_1 INT,
    FOREIGN KEY (id_artist_position_1) REFERENCES Artists(id),
	id_artist_position_2 INT,
    FOREIGN KEY (id_artist_position_2) REFERENCES Artists(id),
	PRIMARY KEY(id_artist_position_1, id_artist_position_2),
    
    users INT
);

/*18. Top artistas entre oyentes— definir oyentes que tienen más de 40 canciones y contar sus
artistas principales.*/
CREATE TABLE IF NOT EXISTS Top_Artists_In_Between_Listeners(
	ranking INT PRIMARY KEY,
	artist_id INT,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

/*19. Popularidad cruzada entre listas — contar artistas que aparecen con más frecuencia en la lista
de canciones vs. la de artistas; reportar la diferencia de conteos.*/
CREATE TABLE IF NOT EXISTS Cross_Popularity(
	artist_id INT PRIMARY KEY,
    FOREIGN KEY (artist_id) REFERENCES Artists(id),
    song_frequency INT,
    artist_frequency INT,
    difference INT
);

/*20. Artistas más diversos — contar cuántos usuarios distintos listan cada artista y cuántas canciones
distintas tiene cada artista en el conjunto.*/
CREATE TABLE IF NOT EXISTS Diverse_Artists(
	artist_id INT PRIMARY KEY,
    FOREIGN KEY (artist_id) REFERENCES Artists(id),
    listeners INT,
    songs INT
);

/*23. Artistas de baja cobertura — contar cuántos artistas aparecen menos de 5 veces.*/
CREATE TABLE IF NOT EXISTS Low_Coverage_Artists(
	artist_id INT PRIMARY KEY,
    FOREIGN KEY (artist_id) REFERENCES Artists(id),
    appearances INT
);