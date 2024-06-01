/* takes in data various sources, and handles converting it to hdf5 format

notes on installation:

./node_modules/.bin/node-gyp configure
./node_modules/.bin/node-gyp build
export DYLD_LIBRARY_PATH=$DYLD_LIBRARY_PATH:/opt/homebrew/Cellar/hdf5/1.14.3_1/lib
export NODE_PATH=/path/to/your/project/node_modules/hdf5/build/Release:$NODE_PATH

*** ^figure out correct NODE_PATH 
npm install hdf5 --hdf5_home_mac=/opt/homebrew/Cellar/hdf5/1.14.3_1
*/

const yahooFinance = require('yahoo-finance2').default;
const fs = require('fs');
const hdf5 = require('hdf5');
const { Access, CreationOrder, H5OType, State } = hdf5.lib;

const DATA_STORE = 'data_store.h5';
const tickers = ['AAPL', 'GOOGL', 'MSFT']; // Example tickers
const PROXY_SERVER = 'http://your-proxy-server.com'; // Example proxy server

const fetchAndStoreHistoryData = async () => {
    const sleepTime = 2500; // Sleep time in milliseconds
    const max_length = Math.max(...tickers.map(t => t.length));
    
    const batchStartTime = Date.now();
    const file = new hdf5.File(DATA_STORE, Access.ACC_TRUNC);
    const group = file.createGroup('yf/minute/us_equity');
    
    for (const t of tickers) {
        try {
            const result = await yahooFinance.historical(t, {
                period1: new Date('2024-05-02'),
                period2: new Date('2024-05-04'),
                interval: '1m',
                includePrePost: true,
                proxy: PROXY_SERVER,
                events: 'div,split'
            });

            if (result.length === 0) {
                console.log(`No data found for ${t}`);
                continue;
            }

            const df = result.map(row => {
                return {
                    datetime: row.date,
                    open: row.open,
                    high: row.high,
                    low: row.low,
                    close: row.close,
                    volume: row.volume,
                    ticker: t
                };
            });

            if (!group.get('prices')) {
                const dset = group.createDataset('prices', new Float64Array(0), {
                    maxLength: max_length,
                    chunkSize: 10,
                    compression: 6
                });
            }

            const dset = group.openDataset('prices');
            dset.write(df);
            console.log(`Added ${t} to store`);
        } catch (e) {
            console.log(`Error fetching data for ${t}: ${e.message}`);
            if (e.message.includes('429')) {
                console.log("429 error detected, delaying to respect rate limit...");
                await new Promise(resolve => setTimeout(resolve, 10000));
            }
        }
        await new Promise(resolve => setTimeout(resolve, sleepTime));
    }

    file.close();
    const batchEndTime = Date.now();
    const batchTotalTime = (batchEndTime - batchStartTime) / 1000;
    console.log(`Batch ran for ${batchTotalTime.toFixed(2)} seconds`);
};

fetchAndStoreHistoryData();