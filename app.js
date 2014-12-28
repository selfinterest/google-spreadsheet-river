/**
 * Created by: Terrence C. Watson
 * Date: 12/19/14
 * Time: 3:17 AM
 */

var riverFactory = require("./lib/main.js");

riverFactory
    .email('759184919979-tfinm66j1hq49b3690039o8mfn60gfe3@developer.gserviceaccount.com')
    .keyFile("./primary-documents-key-file.pem")
    .spreadsheetName("PrimaryDocumentsTest")
    .worksheetName("Sheet1")
    .uniqueKey("csvid")
    .https(true)         //use HTTPS
;

var river = riverFactory.createRiver();


/*river.synchronize().then(function(info){

   console.log(new Date(info.timestamp));
});*/

river.getLastSynchronized().then(function(time){
   //console.log(typeof time);
    console.log(new Date(time));
});
//river.synchronize();