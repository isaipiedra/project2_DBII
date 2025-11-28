/* codigo para actualizar las tablas del querie 9
solo correr si ya tenían la base hecha y solo quieren acutalizar eso
si no habían hecho el docker compose de mysql no es neceario que la corran*/

/* ==========================================================
   UPDATE top_10_Duplicated_Artists
   ========================================================== */

/* Rename old column */
ALTER TABLE top_10_Duplicated_Artists
RENAME COLUMN artist_id TO artist_id_1;

/* Add new columns (usar VARCHAR(64) porque Artists.id es VARCHAR(64)) */
ALTER TABLE top_10_Duplicated_Artists
ADD COLUMN artist_id_2 VARCHAR(64),
ADD COLUMN artist_id_3 VARCHAR(64),
ADD COLUMN total_users INT;

/* Add new foreign keys */
ALTER TABLE top_10_Duplicated_Artists
ADD CONSTRAINT fk_artist2 FOREIGN KEY (artist_id_2) REFERENCES Artists(id),
ADD CONSTRAINT fk_artist3 FOREIGN KEY (artist_id_3) REFERENCES Artists(id);


/* ==========================================================
   UPDATE top_10_Duplicated_Albums
   ========================================================== */

/* Rename old column */
ALTER TABLE top_10_Duplicated_Albums
RENAME COLUMN album_id TO album_id_1;

/* Add new columns */
ALTER TABLE top_10_Duplicated_Albums
ADD COLUMN album_id_2 VARCHAR(64),
ADD COLUMN album_id_3 VARCHAR(64),
ADD COLUMN total_users INT;

/* Add new foreign keys */
ALTER TABLE top_10_Duplicated_Albums
ADD CONSTRAINT fk_album2 FOREIGN KEY (album_id_2) REFERENCES Albums(id),
ADD CONSTRAINT fk_album3 FOREIGN KEY (album_id_3) REFERENCES Albums(id);


/* ==========================================================
   UPDATE top_10_Duplicated_Songs
   ========================================================== */

/* Rename old column */
ALTER TABLE top_10_Duplicated_Songs
RENAME COLUMN song_id TO song_id_1;

/* Add new columns */
ALTER TABLE top_10_Duplicated_Songs
ADD COLUMN song_id_2 VARCHAR(64),
ADD COLUMN song_id_3 VARCHAR(64),
ADD COLUMN total_users INT;

/* Add new foreign keys */
ALTER TABLE top_10_Duplicated_Songs
ADD CONSTRAINT fk_song2 FOREIGN KEY (song_id_2) REFERENCES Songs(id),
ADD CONSTRAINT fk_song3 FOREIGN KEY (song_id_3) REFERENCES Songs(id);


/* ==========================================================
   UPDATE top_50_Paired_Artists
   ========================================================== */

ALTER TABLE top_50_Paired_Artists
ADD COLUMN total_users INT;


/* ==========================================================
   UPDATE top_20_Trio_Artists
   ========================================================== */

ALTER TABLE top_20_Trio_Artists
ADD COLUMN total_users INT;
