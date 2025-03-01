const { Pool } = require('pg');

// Database connection configuration
const pool = new Pool({
    user: 'postgres',
    password: 'postgres',
    host: 'localhost',
    database: 'saturday',
    port: 5432,
});

// Basic connection test
async function testDatabaseConnection()
{
    try {
        const client = await pool.connect();
        console.log('Connected to PostgreSQL database successfully!');

        // Simple query to test the connection
        const result = await client.query('SELECT NOW()');
        console.log('PostgreSQL server time:', result.rows[0].now);

        client.release();
    } catch (err) {
        console.error('Error connecting to the database:', err);
    } finally {
        await pool.end();
    }
}

testDatabaseConnection();
