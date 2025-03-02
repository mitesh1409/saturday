const db = require('./db');

// Basic database connection test
async function testDatabaseConnection()
{
    let dbClient;

    try {
        dbClient = await db.getClient();
        console.log('Connected to PostgreSQL database successfully!');

        // Simple query to test the connection
        const result = await dbClient.query('SELECT NOW()');
        console.log('PostgreSQL server time:', result.rows[0].now);
    } catch (err) {
        console.error('Error connecting to the database:', err);
    } finally {
        dbClient.release();
        db.closePool();
    }
}

testDatabaseConnection();
