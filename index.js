const db = require('./db');
const fs = require('fs');
const { parse } = require('csv-parse');

const stockData = [];

fs.createReadStream('./data-source/Quote-Equity-TCS-EQ-01-01-2024-to-31-12-2024.csv')
    .pipe(parse({
        comment: '#',
        columns: true,
        bom: true,  // Add this line to handle UTF-8 BOM
        trim: true, // Add this to trim whitespace from fields
        skip_empty_lines: true,
        relax_quotes: true // More lenient quote handling
    }))
    .on('data', (data) => {
        console.log('data', data);
        stockData.push(data);
    })
    .on('error', (error) => {
        console.log('Error while reading the file!');
        console.log('Error:', error);
    })
    .on('end', () => {
        console.log('Completed reading the file');
        console.log(`${stockData.length} records found!`);
    });

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
