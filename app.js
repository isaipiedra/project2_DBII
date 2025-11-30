const express = require('express');
const path = require('path');
const { executeQuery } = require('./database');

const app = express();
const port = 3000;

app.use(express.json());
app.use(express.static('public'));

// API endpoint for Top 20 General Artists
app.get('/api/top-artists', async (req, res) => {
    try {
        const sql = `
            SELECT art.name, top.listeners, top.participation_percentage 
            FROM metrics.Top_20_General_Artists top 
            JOIN metrics.Artists art ON top.artist_id = art.id 
            ORDER BY top.ranking ASC
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching top artists:', error);
        res.status(500).json({ error: 'Failed to fetch top artists' });
    }
});

// API endpoint for Top 20 General Songs
app.get('/api/top-songs', async (req, res) => {
    try {
        const sql = `
            SELECT sng.name, top.listeners, top.participation_percentage 
            FROM metrics.Top_20_General_Songs top 
            JOIN metrics.Songs sng ON top.song_id = sng.id 
            ORDER BY top.ranking ASC
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching top songs:', error);
        res.status(500).json({ error: 'Failed to fetch top songs' });
    }
});

// API endpoint for Top 20 General Albums
app.get('/api/top-albums', async (req, res) => {
    try {
        const sql = `
            SELECT alb.name, top.listeners, top.participation_percentage 
            FROM metrics.Top_20_General_Albums top 
            JOIN metrics.Albums alb ON top.album_id = alb.id 
            ORDER BY top.ranking ASC
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching top albums:', error);
        res.status(500).json({ error: 'Failed to fetch top albums' });
    }
});

// API endpoint for Query 4 - Same Top One Artist
app.get('/api/same-top-artist', async (req, res) => {
    try {
        const sql = `
            SELECT art.name, top.frequency 
            FROM metrics.Same_Top_One_Artist top 
            JOIN metrics.Artists art ON top.artist_id = art.id 
            ORDER BY top.frequency DESC 
            LIMIT 1
        `;
        const results = await executeQuery(sql);
        res.json(results[0] || {});
    } catch (error) {
        console.error('Error fetching same top artist:', error);
        res.status(500).json({ error: 'Failed to fetch same top artist' });
    }
});

// API endpoint for Query 5 - Mentions Per Artist
app.get('/api/mentions-artist', async (req, res) => {
    try {
        const sql = `
            SELECT art.name, top.mentions 
            FROM metrics.Mentions_Per_Artist top 
            JOIN metrics.Artists art ON top.artist_id = art.id 
            ORDER BY top.mentions DESC 
            LIMIT 100
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching mentions per artist:', error);
        res.status(500).json({ error: 'Failed to fetch mentions per artist' });
    }
});

// API endpoint for Query 5 - Metrics
app.get('/api/mentions-metrics', async (req, res) => {
    try {
        const sql = `
            SELECT average, median, standard_deviation 
            FROM metrics.Metrics 
            WHERE query = 5
        `;
        const results = await executeQuery(sql);
        res.json(results[0] || {});
    } catch (error) {
        console.error('Error fetching mentions metrics:', error);
        res.status(500).json({ error: 'Failed to fetch mentions metrics' });
    }
});

// API endpoint for Query 6 - Long Tail
app.get('/api/long-tail', async (req, res) => {
    try {
        const sql = `
            SELECT value 
            FROM metrics.Single_Value_Queries 
            WHERE query = 6
        `;
        const results = await executeQuery(sql);
        res.json(results[0] || {});
    } catch (error) {
        console.error('Error fetching long tail:', error);
        res.status(500).json({ error: 'Failed to fetch long tail' });
    }
});

// API endpoint for Query 7 - Items per user metrics
app.get('/api/items-per-user-metrics', async (req, res) => {
    try {
        const sql = `
            SELECT average, median 
            FROM metrics.Metrics 
            WHERE query = 7
        `;
        const results = await executeQuery(sql);
        res.json(results[0] || {});
    } catch (error) {
        console.error('Error fetching items per user metrics:', error);
        res.status(500).json({ error: 'Failed to fetch items per user metrics' });
    }
});

// API endpoint for Query 8 - Unique Items
app.get('/api/unique-items', async (req, res) => {
    try {
        const sql = `
            SELECT user_id, artists, songs, albums 
            FROM metrics.Unique_Items 
            ORDER BY artists ASC
            LIMIT 100
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching unique items:', error);
        res.status(500).json({ error: 'Failed to fetch unique items' });
    }
});

// API endpoint for Query 9.1 - Top 10 duplicated artists
app.get('/api/duplicated-artists', async (req, res) => {
    try {
        const sql = `
            SELECT art_1.name as artist1, art_2.name as artist2, art_3.name as artist3, top.total_users 
            FROM metrics.top_10_Duplicated_Artists top 
            JOIN metrics.Artists art_1 ON top.artist_id_1 = art_1.id
            JOIN metrics.Artists art_2 ON top.artist_id_2 = art_2.id
            JOIN metrics.Artists art_3 ON top.artist_id_3 = art_3.id
            ORDER BY top.ranking
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching duplicated artists:', error);
        res.status(500).json({ error: 'Failed to fetch duplicated artists' });
    }
});

// API endpoint for Query 9.2 - Top 10 duplicated albums
app.get('/api/duplicated-albums', async (req, res) => {
    try {
        const sql = `
            SELECT alb_1.name as album1, alb_2.name as album2, alb_3.name as album3, top.total_users 
            FROM metrics.top_10_Duplicated_Albums top 
            JOIN metrics.Albums alb_1 ON top.album_id_1 = alb_1.id
            JOIN metrics.Albums alb_2 ON top.album_id_2 = alb_2.id
            JOIN metrics.Albums alb_3 ON top.album_id_3 = alb_3.id
            ORDER BY top.ranking
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching duplicated albums:', error);
        res.status(500).json({ error: 'Failed to fetch duplicated albums' });
    }
});

// API endpoint for Query 9.3 - Top 10 duplicated songs
app.get('/api/duplicated-songs', async (req, res) => {
    try {
        const sql = `
            SELECT sng_1.name as song1, sng_2.name as song2, sng_3.name as song3, top.total_users 
            FROM metrics.top_10_Duplicated_Songs top 
            JOIN metrics.Songs sng_1 ON top.song_id_1 = sng_1.id
            JOIN metrics.Songs sng_2 ON top.song_id_2 = sng_2.id
            JOIN metrics.Songs sng_3 ON top.song_id_3 = sng_3.id
            ORDER BY top.ranking
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching duplicated songs:', error);
        res.status(500).json({ error: 'Failed to fetch duplicated songs' });
    }
});

// API endpoint for Query 10 - Loyal listeners
app.get('/api/loyal-listeners', async (req, res) => {
    try {
        const sql = `
            SELECT value 
            FROM metrics.Single_Value_Queries 
            WHERE query = 10
        `;
        const results = await executeQuery(sql);
        res.json(results[0] || {});
    } catch (error) {
        console.error('Error fetching loyal listeners:', error);
        res.status(500).json({ error: 'Failed to fetch loyal listeners' });
    }
});

// API endpoint for Query 11 - Top 50 paired artists
app.get('/api/paired-artists', async (req, res) => {
    try {
        const sql = `
            SELECT art_1.name as artist1, art_2.name as artist2, top.total_users
            FROM metrics.top_50_Paired_Artists top
            JOIN metrics.Artists art_1 ON top.artist_1_id = art_1.id
            JOIN metrics.Artists art_2 ON top.artist_2_id = art_2.id
            ORDER BY top.ranking
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching paired artists:', error);
        res.status(500).json({ error: 'Failed to fetch paired artists' });
    }
});

// API endpoint for Query 12 - Top 20 trio artists
app.get('/api/trio-artists', async (req, res) => {
    try {
        const sql = `
            SELECT art_1.name as artist1, art_2.name as artist2, art_3.name as artist3, top.total_users 
            FROM metrics.top_20_Trio_Artists top 
            JOIN metrics.Artists art_1 ON top.artist_1_id = art_1.id
            JOIN metrics.Artists art_2 ON top.artist_2_id = art_2.id
            JOIN metrics.Artists art_3 ON top.artist_3_id = art_3.id
            ORDER BY top.ranking
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching trio artists:', error);
        res.status(500).json({ error: 'Failed to fetch trio artists' });
    }
});

/**
 * Starts the server and initializes database connection
 * @function start_server
 * @returns {void}
 */
async function start_server() {
    try {
        app.listen(port, () => {
            console.log(`Dashboard app running at http://localhost:${port}`);
        });
    } catch (err) {
        console.error('Failed to start server:', err);
    }
}

start_server();