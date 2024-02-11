const {Storage} = require('@google-cloud/storage');
const {BigQuery} = require('@google-cloud/bigquery')

const bq = new BigQuery();
const datasetId = 'weather_etl';
// const tableId = 'weather_station_data';
const tableId = 'weather_data';

const csv = require('csv-parser');
const nullValue= -9999
const decimalValue= ['airtemp', 'dewpoint', 'pressure', 'windspped', 'precip1hour', 'precip6hour']
const nonDec = ['station','year','month','day','hour','winddirection','sky']
let i = 13
exports.readObservation = (file, context) => {
    const gcs = new Storage();

    const dataFile= gcs.bucket(file.bucket).file(file.name);
    dataFile.createReadStream()
    .on('error', () => {
        ///Handle an error
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        let fileName = file.name.split(".")[0];
        row.station=fileName;
        //^ appends station name to station feild
        for (let key in row){
            if (nonDec.includes(key)){
                if (row[key] == (nullValue)){
                    row[key] = null;
                }
            }
        }
        for (let key in row){
            if (decimalValue.includes(key)){
                if (row[key] == nullValue){
                    row[key] = null;
                } else {
                row[key] = row[key] /10;
            }
            }
            
            //^ Checks for null values
            //^Changes values to decimals
        }
            
        

        //Log row data
        // console.log(row);
        console.log(row)
        printDict(row);

        const weahter_data_obj = () => {
            //Create a fakey object
            weatherObject = {};
            weatherObject.station = row.station;
            weatherObject.year = parseInt(row.year);
            weatherObject.month = parseInt(row.month);
            weatherObject.day = parseInt(row.day);
            weatherObject.hour = parseInt(row.hour);
            weatherObject.airtemp = row.airtemp;
            weatherObject.dewpoint = row.dewpoint;
            weatherObject.pressure = row.pressure;
            weatherObject.winddirection = row.winddirection;
            weatherObject.windspeed = row.windspeed;
            weatherObject.sky = row.sky;
            weatherObject.precip1hour = row.precip1hour;
            weatherObject.precip6hour = row.precip6hour;


            writeToBq(weatherObject);
        }
        
        
        
        //call that entry point function
        weahter_data_obj();
        
        
        // Create a helper function that writes to BQ
        // Function must be asynchronous
        async function writeToBq(obj) {
            //BQ expects an arrary of objects, but this function only receives 1
            var rows = []; //Empty array
            rows.push(obj);
        
            // Insert the array of objects into the 'demo' table
            await bq
            .dataset(datasetId)
            .table(tableId)
            .insert(rows)
            .then( () => {
                rows.forEach ( (row) => { console.log(`Inseerted: ${row}`)})
            })
            .catch( (err)=> { console.error(`ERROR: ${err}`) } )
        
        }
        
    })
    .on('end', () => {
        //Handle end of CSV
        console.log('End!');
    })


}

//HELPER FUNCTIONS

function printDict(row){
    for (let key in row){
        console.log(key + ' : '+ row [key]);
        //second one does same thing
        // console.log(`${key} : ${row[key]}`);
    }
}
