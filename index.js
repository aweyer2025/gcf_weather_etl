const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');
const nullValue= -9999
const decimalValue= ['airtemp', 'dewpoint', 'pressure', 'windspped', 'precip1hour', 'precip6hour']
let i = 13
exports.readObservation = (file, context) => {
    // console.log(`  Event: ${context.eventId}`);
    // console.log(`  Event Type: ${context.eventType}`);
    // console.log(`  Bucket: ${file.bucket}`);
    // console.log(`  File: ${file.name}`);
    const gcs = new Storage();

    const dataFile= gcs.bucket(file.bucket).file(file.name);
    // const valueToNull = (value) => {
    //     integerValue = null;
    // }
    dataFile.createReadStream()
    .on('error', () => {
        ///Handle an error
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        let fileName = file.name.replace('.0.csv','')
        row.station=fileName;
        //^ appends station name to station feild
        for (let key in row){
            if (row[key] == nullValue){
                row[key] = null;
            }
            //^ Checks for null values
            if (decimalValue.includes(key)){
                row[key] = row[key] /10;
            }
            //^Changes values to decimals
        }
            
        

        //Log row data
        // console.log(row);
        console.log(row)
        printDict(row);

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