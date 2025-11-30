const mysql = require('mysql2/promise');

const dbConfig = {
    host: 'localhost',
    port: 3307,
    user: 'root',
    password: 'BDII_PROYII',
    database: 'metrics'
};

/**
 * Creates a connection to the MySQL database
 * @returns {Promise<mysql.Connection>} Database connection
 */
async function getConnection() {
    try {
        const connection = await mysql.createConnection(dbConfig);
        return connection;
    } catch (error) {
        console.error('Database connection failed:', error);
        throw error;
    }
}

/**
 * Executes a SQL query with parameters
 * @param {string} sql - SQL query string
 * @param {Array} params - Query parameters
 * @returns {Promise<Array>} Query results
 */
async function executeQuery(sql, params = []) {
    let connection;
    try {
        connection = await getConnection();
        const [results] = await connection.execute(sql, params);
        return results;
    } catch (error) {
        console.error('Query execution failed:', error);
        throw error;
    } finally {
        if (connection) {
            await connection.end();
        }
    }
}

module.exports = {
    getConnection,
    executeQuery
};