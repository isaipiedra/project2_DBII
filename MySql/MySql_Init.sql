CREATE DATABASE IF NOT EXISTS metrics;
USE metrics;

-- ============================
-- TABLAS BASE
-- ============================

CREATE TABLE IF NOT EXISTS Artists (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS Albums (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS Songs (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(500)
);

-- Métricas generales
CREATE TABLE IF NOT EXISTS Metrics(
    query INT PRIMARY KEY,
    average DOUBLE,
    median DOUBLE,
    standard_deviation DOUBLE
);

-- Consultas que devuelven 1 valor
CREATE TABLE IF NOT EXISTS Single_Value_Queries(
    query INT PRIMARY KEY,
    value DOUBLE
);

-- ============================
-- 1. Top 20 artistas globales
-- ============================

CREATE TABLE IF NOT EXISTS Top_20_General_Artists(
    ranking INT PRIMARY KEY,
    artist_id VARCHAR(64),
    listeners INT,
    participation_percentage DOUBLE,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

-- ============================
-- 2. Top 20 canciones globales
-- ============================

CREATE TABLE IF NOT EXISTS Top_20_General_Songs(
    ranking INT PRIMARY KEY,
    song_id VARCHAR(64),
    listeners INT,
    participation_percentage DOUBLE,
    FOREIGN KEY (song_id) REFERENCES Songs(id)
);

-- ============================
-- 3. Top 20 álbumes globales
-- ============================

CREATE TABLE IF NOT EXISTS Top_20_General_Albums(
    ranking INT PRIMARY KEY,
    album_id VARCHAR(64),
    listeners INT,
    participation_percentage DOUBLE,
    FOREIGN KEY (album_id) REFERENCES Albums(id)
);

-- ============================
-- 4. Usuarios que comparten su artista top-1
-- ============================

CREATE TABLE IF NOT EXISTS Same_Top_One_Artist(
    artist_id VARCHAR(64) PRIMARY KEY,
    frequency INT,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

-- ============================
-- 5. Distribución de menciones por artista
-- ============================

CREATE TABLE IF NOT EXISTS Mentions_Per_Artist(
    artist_id VARCHAR(64) PRIMARY KEY,
    mentions INT,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

-- ============================
-- 6. Participación del long tail
-- ============================

CREATE TABLE IF NOT EXISTS Long_Tail(
    artist_id VARCHAR(64) PRIMARY KEY,
    mentions_percentage DOUBLE,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

-- ============================
-- 7. Ítems por usuario
-- ============================

CREATE TABLE IF NOT EXISTS Items_Per_User(
    user_id INT PRIMARY KEY,
    items INT
);

-- ============================
-- 8. Artistas/canciones/álbumes distintos
-- ============================

CREATE TABLE IF NOT EXISTS Unique_Items(
    user_id INT PRIMARY KEY,
    artists INT,
    songs INT,
    albums INT
);

-- ============================
-- 9. Usuarios con listas top-10 duplicadas
-- ============================

CREATE TABLE IF NOT EXISTS top_10_Duplicated_Artists(
	ranking INT PRIMARY KEY,
	artist_id_1 INT,
    FOREIGN KEY (artist_id_1) REFERENCES Artists(id),
    artist_id_2 INT,
    FOREIGN KEY (artist_id_2) REFERENCES Artists(id),
    artist_id_3 INT,
    FOREIGN KEY (artist_id_3) REFERENCES Artists(id),
    total_users INT
);

CREATE TABLE IF NOT EXISTS top_10_Duplicated_Albums(
	ranking INT PRIMARY KEY,
	album_id_1 INT,
    FOREIGN KEY (album_id_1) REFERENCES Albums(id),
    album_id_2 INT,
    FOREIGN KEY (album_id_2) REFERENCES Albums(id),
    album_id_3 INT,
    FOREIGN KEY (album_id_3) REFERENCES Albums(id),
    total_users INT
);

CREATE TABLE IF NOT EXISTS top_10_Duplicated_Songs(
	ranking INT PRIMARY KEY,
	song_id_1 INT,
    FOREIGN KEY (song_id_1) REFERENCES Songs(id),
    song_id_2 INT,
    FOREIGN KEY (song_id_2) REFERENCES Songs(id),
    song_id_3 INT,
    FOREIGN KEY (song_id_3) REFERENCES Songs(id),
    total_users INT
);

-- ============================
-- 10. Usuarios con top 5 del mismo artista
-- ============================

CREATE TABLE IF NOT EXISTS Loyal_Listeners(
    artist_id VARCHAR(64) PRIMARY KEY,
    loyal_listeners INT,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

-- ============================
-- 11. Pares de artistas más frecuentes
-- ============================

CREATE TABLE IF NOT EXISTS top_50_Paired_Artists(
    ranking INT PRIMARY KEY,
    artist_1_id VARCHAR(64),
    artist_2_id VARCHAR(64),
    FOREIGN KEY (artist_1_id) REFERENCES Artists(id),
    FOREIGN KEY (artist_2_id) REFERENCES Artists(id)
);

-- ============================
-- 12. Tripletas de artistas más frecuentes
-- ============================

CREATE TABLE IF NOT EXISTS top_20_Trio_Artists(
    ranking INT PRIMARY KEY,
    artist_1_id VARCHAR(64),
    artist_2_id VARCHAR(64),
    artist_3_id VARCHAR(64),
    FOREIGN KEY (artist_1_id) REFERENCES Artists(id),
    FOREIGN KEY (artist_2_id) REFERENCES Artists(id),
    FOREIGN KEY (artist_3_id) REFERENCES Artists(id)
);

-- ============================
-- 14. Posición promedio por artista
-- ============================

CREATE TABLE IF NOT EXISTS Average_Artist_Position(
    artist_id VARCHAR(64) PRIMARY KEY,
    average DOUBLE,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

-- ============================
-- 15. Correlación entre top-1 y top 5 global
-- ============================

CREATE TABLE IF NOT EXISTS Global_Top_5_Correlates_Top_Per_User(
    ranking INT PRIMARY KEY,
    artist_id VARCHAR(64),
    users INT,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

-- ============================
-- 16. Estabilidad de posiciones 1 y 2
-- ============================

CREATE TABLE IF NOT EXISTS Same_Top_1_And_2(
    id_artist_position_1 VARCHAR(64),
    id_artist_position_2 VARCHAR(64),
    users INT,
    PRIMARY KEY(id_artist_position_1, id_artist_position_2),
    FOREIGN KEY (id_artist_position_1) REFERENCES Artists(id),
    FOREIGN KEY (id_artist_position_2) REFERENCES Artists(id)
);

-- ============================
-- 18. Top artistas entre oyentes intensivos
-- ============================

CREATE TABLE IF NOT EXISTS Top_Artists_In_Between_Listeners(
    ranking INT PRIMARY KEY,
    artist_id VARCHAR(64),
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

-- ============================
-- 19. Popularidad cruzada entre listas
-- ============================

CREATE TABLE IF NOT EXISTS Cross_Popularity(
    artist_id VARCHAR(64) PRIMARY KEY,
    song_frequency INT,
    artist_frequency INT,
    difference INT,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

-- ============================
-- 20. Artistas más diversos
-- ============================

CREATE TABLE IF NOT EXISTS Diverse_Artists(
    artist_id VARCHAR(64) PRIMARY KEY,
    listeners INT,
    songs INT,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);

-- ============================
-- 23. Artistas de baja cobertura
-- ============================

CREATE TABLE IF NOT EXISTS Low_Coverage_Artists(
    artist_id VARCHAR(64) PRIMARY KEY,
    appearances INT,
    FOREIGN KEY (artist_id) REFERENCES Artists(id)
);
