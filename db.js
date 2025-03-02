const { Pool } = require('pg');

// Create a pool instance
const pool = new Pool({
    user: 'postgres',
    password: 'postgres',
    host: 'localhost',
    database: 'saturday',
    port: 5432,
});

// Get a client from the pool
async function getClient() {
    const client = await pool.connect();
    return client;
}

// Execute a query using the pool directly
async function query(text, params) {
    const result = await pool.query(text, params);
    return result;
}

// Clean up function - call this when shutting down your app
async function closePool() {
    await pool.end();
}

module.exports = {
    pool,
    getClient,
    query,
    closePool
};