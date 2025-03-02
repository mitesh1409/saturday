const db = require('./db');
const fs = require('fs');
const { parse } = require('csv-parse');

// Function to create asset_data table
async function createAssetDataTable() {
    const dbClient = await db.getClient();

    try {
        // Create the asset_data table
        await dbClient.query(`
            CREATE TABLE IF NOT EXISTS asset_data (
                id SERIAL PRIMARY KEY,
                date DATE,
                series VARCHAR(10),
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                prev_close NUMERIC,
                ltp NUMERIC,
                vwap NUMERIC,
                "52w_high" NUMERIC,
                "52w_low" NUMERIC,
                volume BIGINT,
                value NUMERIC,
                no_of_trades INTEGER
            )
        `);
        console.log('Table asset_data created or already exists');
    } catch (err) {
        console.error('Error creating table:', err);
        throw err;
    } finally {
        dbClient.release();
    }
}

// Function to process the CSV data and insert into database
async function importAssetData() {
    const assetData = [];

    // Parse CSV file
    await new Promise((resolve, reject) => {
        fs.createReadStream('./data-source/Quote-Equity-TCS-EQ-01-01-2024-to-31-12-2024.csv')
            .pipe(parse({
                comment: '#',
                columns: true,
                bom: true,
                trim: true,
                skip_empty_lines: true,
                relax_quotes: true
            }))
            .on('data', (data) => {
                assetData.push(data);
            })
            .on('error', (error) => {
                console.log('Error while reading the file:', error);
                reject(error);
            })
            .on('end', () => {
                console.log(`Completed reading the file. ${assetData.length} records found!`);
                resolve();
            });
    });

    // Insert data into database
    if (assetData.length > 0) {
        const client = await db.getClient();

        try {
            // Begin transaction
            await client.query('BEGIN');

            for (const record of assetData) {
                // Process and clean the data
                const cleanedRecord = {
                    date: parseDate(record['Date']),
                    series: record['series'],
                    open: parseNumber(record['OPEN']),
                    high: parseNumber(record['HIGH']),
                    low: parseNumber(record['LOW']),
                    prev_close: parseNumber(record['PREV. CLOSE']),
                    ltp: parseNumber(record['ltp']),
                    close: parseNumber(record['close']),
                    vwap: parseNumber(record['vwap']),
                    "52w_high": parseNumber(record['52W H']),
                    "52w_low": parseNumber(record['52W L']),
                    volume: parseInteger(record['VOLUME']),
                    value: parseNumber(record['VALUE']),
                    no_of_trades: parseInteger(record['No of trades'])
                };

                // Insert into database
                await client.query(`
                    INSERT INTO asset_data 
                    (date, series, open, high, low, close, prev_close, ltp, vwap, "52w_high", "52w_low", volume, value, no_of_trades)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    `, [
                            cleanedRecord.date,
                            cleanedRecord.series,
                            cleanedRecord.open,
                            cleanedRecord.high,
                            cleanedRecord.low,
                            cleanedRecord.close,
                            cleanedRecord.prev_close,
                            cleanedRecord.ltp,
                            cleanedRecord.vwap,
                            cleanedRecord["52w_high"],
                            cleanedRecord["52w_low"],
                            cleanedRecord.volume,
                            cleanedRecord.value,
                            cleanedRecord.no_of_trades
                ]);
            }

            // Commit transaction
            await client.query('COMMIT');
            console.log(`Successfully inserted ${assetData.length} records into asset_data table`);
        } catch (err) {
            await client.query('ROLLBACK');
            console.error('Error inserting data:', err);
            throw err;
        } finally {
            client.release();
        }
    }
}

// Helper function to parse date from DD-MMM-YYYY format
function parseDate(dateString) {
    if (!dateString) return null;

    // Remove quotes and trim
    dateString = dateString.replace(/"/g, '').trim();

    // Parse DD-MMM-YYYY format
    const [day, month, year] = dateString.split('-');

    if (!day || !month || !year) return null;

    return `${year}-${month}-${day}`;
}

// Helper function to parse numeric values that might contain commas
function parseNumber(value) {
    if (!value) return null;

    // Remove quotes, commas and trim
    const cleanValue = value.replace(/"/g, '').replace(/,/g, '').trim();

    if (cleanValue === '' || isNaN(cleanValue)) return null;

    return parseFloat(cleanValue);
}

// Helper function to parse integer values that might contain commas
function parseInteger(value) {
    if (!value) return null;

    // Remove quotes, commas and trim
    const cleanValue = value.replace(/"/g, '').replace(/,/g, '').trim();

    if (cleanValue === '' || isNaN(cleanValue)) return null;

    return parseInt(cleanValue, 10);
}

// Main function to run everything
async function main() {
    try {
        // Create table
        await createAssetDataTable();

        // Import data
        await importAssetData();

        console.log('Data import completed successfully');
    } catch (err) {
        console.error('Error in main process:', err);
    } finally {
        // Close database connection pool
        await db.closePool();
    }
}

// Run the main function
main();
