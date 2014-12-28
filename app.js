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


/*river.synchronize().then(function(){
    console.log(arguments);
   console.log("Done");
});*/

river.getLastSynchronized().then(function(time){
    if(time){
        console.log(new Date(time));
    } else {
        console.log("Not synchronized.");
    }
   //console.log(typeof time);

});

river.getNumberOfKeys().then(function(keys){
    console.log(keys);
});

river.getAllValuesFromRedis().then(function(result){
    console.log(result.keys);
});
