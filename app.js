const express = require('express');
const path = require('path');
const { executeQuery } = require('./database');

const app = express();
const port = 3000;

app.use(express.json());
app.use(express.static('public'));

// ============ TAB 1: POPULARITY AND FREQUENCY ============

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

// ============ TAB 2: SIMPLE COUNTS BY USER ============

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

// ============ TAB 3: CONCURRENCY AND POSITIONS ============

app.get('/api/artist-song-overlap', async (req, res) => {
    try {
        const sql = `
            SELECT value 
            FROM metrics.Single_Value_Queries
            WHERE query = 13
        `;
        const results = await executeQuery(sql);
        res.json(results[0] || {});
    } catch (error) {
        console.error('Error fetching artist-song overlap:', error);
        res.status(500).json({ error: 'Failed to fetch artist-song overlap' });
    }
});

app.get('/api/average-artist-position', async (req, res) => {
    try {
        const sql = `
            SELECT art.name, pos.average
            FROM metrics.Average_Artist_Position pos
            JOIN metrics.Artists art ON pos.artist_id = art.id
            ORDER BY pos.average DESC
            LIMIT 100
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching average artist position:', error);
        res.status(500).json({ error: 'Failed to fetch average artist position' });
    }
});

app.get('/api/top5-correlation', async (req, res) => {
    try {
        const sql = `
            SELECT art.name, top.users
            FROM metrics.Global_Top_5_Correlates_Top_Per_User top
            JOIN metrics.Artists art ON top.artist_id = art.id
            ORDER BY top.users DESC
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching top 5 correlation:', error);
        res.status(500).json({ error: 'Failed to fetch top 5 correlation' });
    }
});

app.get('/api/same-top1-top2', async (req, res) => {
    try {
        const sql = `
            SELECT art_1.name as artist1, art_2.name as artist2, top.users 
            FROM metrics.Same_Top_1_And_2 top
            JOIN metrics.Artists art_1 ON top.id_artist_position_1 = art_1.id
            JOIN metrics.Artists art_2 ON top.id_artist_position_2 = art_2.id
            ORDER BY top.users DESC
            LIMIT 100
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching same top 1 and 2 artists:', error);
        res.status(500).json({ error: 'Failed to fetch same top 1 and 2 artists' });
    }
});

// ============ TAB 4: SIMPLE COMPARISONS ============

app.get('/api/top-artists-listeners', async (req, res) => {
    try {
        const sql = `
            SELECT top.ranking, art.name 
            FROM metrics.Top_Artists_In_Between_Listeners top
            JOIN metrics.Artists art ON top.artist_id = art.id
            ORDER BY top.ranking
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching top artists among listeners:', error);
        res.status(500).json({ error: 'Failed to fetch top artists among listeners' });
    }
});

app.get('/api/crossed-popularity', async (req, res) => {
    try {
        const sql = `
            SELECT art.name, crs.song_frequency, crs.artist_frequency, crs.difference 
            FROM metrics.Cross_Popularity crs
            JOIN metrics.Artists art ON crs.artist_id = art.id
            ORDER BY crs.difference DESC
            LIMIT 100
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching crossed popularity:', error);
        res.status(500).json({ error: 'Failed to fetch crossed popularity' });
    }
});

app.get('/api/diverse-artists', async (req, res) => {
    try {
        const sql = `
            SELECT art.name, dv.listeners, dv.songs 
            FROM metrics.Diverse_Artists dv
            JOIN metrics.Artists art ON dv.artist_id = art.id
            ORDER BY dv.listeners DESC
            LIMIT 100
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching diverse artists:', error);
        res.status(500).json({ error: 'Failed to fetch diverse artists' });
    }
});

// ============ TAB 5: QUALITY ============

app.get('/api/missing-data', async (req, res) => {
    try {
        const sql = `
            SELECT value FROM metrics.Single_Value_Queries WHERE query = 21
        `;
        const results = await executeQuery(sql);
        res.json(results[0] || {});
    } catch (error) {
        console.error('Error fetching missing data count:', error);
        res.status(500).json({ error: 'Failed to fetch missing data count' });
    }
});

app.get('/api/atypical-users', async (req, res) => {
    try {
        const sql = `
            SELECT value FROM metrics.Single_Value_Queries WHERE query = 22
        `;
        const results = await executeQuery(sql);
        res.json(results[0] || {});
    } catch (error) {
        console.error('Error fetching atypical users count:', error);
        res.status(500).json({ error: 'Failed to fetch atypical users count' });
    }
});

app.get('/api/low-coverage-count', async (req, res) => {
    try {
        const sql = `
            SELECT value FROM metrics.Single_Value_Queries WHERE query = 23
        `;
        const results = await executeQuery(sql);
        res.json(results[0] || {});
    } catch (error) {
        console.error('Error fetching low coverage count:', error);
        res.status(500).json({ error: 'Failed to fetch low coverage count' });
    }
});

app.get('/api/low-coverage-artists', async (req, res) => {
    try {
        const sql = `
            SELECT art.name, low.appearances
            FROM metrics.Low_Coverage_Artists low
            JOIN metrics.Artists art ON low.artist_id = art.id
            ORDER BY low.appearances
            LIMIT 100
        `;
        const results = await executeQuery(sql);
        res.json(results);
    } catch (error) {
        console.error('Error fetching low coverage artists:', error);
        res.status(500).json({ error: 'Failed to fetch low coverage artists' });
    }
});

// ============ SERVER INITIALIZATION ============

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