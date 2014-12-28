/**
 * Created by: Terrence C. Watson
 * Date: 12/19/14
 * Time: 3:19 AM
 */

var Dispatcher = require('flux').Dispatcher;
var assign = require("object-assign");
var Redis = require("redis-stream");
var gsStreamFactory = require("google-spreadsheet-stream-reader").factory;

var optionsObject = {
    expires: Date.now() + 3600000,
    email: null,
    keyFile: null,
    spreadsheetName: null,
    worksheetName: null,
    https: true,
    uniqueKey: "filename",
    DB: "gss_river",
    DBPort: 6379,
    DBHost: "localhost"
};

function GoogleSpreadsheetRiver(options){
    if(this instanceof GoogleSpreadsheetRiver){
        for(var k in optionsObject){
            if(optionsObject.hasOwnProperty(k)){
                this[k] = optionsObject[k];
            }
        }

        //Create a gsReadStream
        this.gsReadStream = gsStreamFactory
            .email(this.email)
            .keyFile(this.keyFile)
            .spreadsheetName(this.spreadsheetName)
            .worksheetName(this.worksheetName)
            .https(this.https)
        ;

    } else {
        return new GoogleSpreadsheetRiver(options);
    }

}


GoogleSpreadsheetRiver.prototype.synchronize = function(){

};

GoogleSpreadsheetRiver.prototype.handleViewAction = function(action){
    this.dispatch({
        source: "VIEW_ACTION",
        action: action
    })
};

//Create option setter/getters
for(var k in optionsObject){

    if(optionsObject.hasOwnProperty(k)){
        GoogleSpreadsheetRiver[k] = function(optionsObjectProperty, option){
            if(option){
                optionsObject[optionsObjectProperty] = option;
                return this;
            } else {
                return optionsObject[optionsObjectProperty];
            }
        }.bind(GoogleSpreadsheetRiver, k);
    }
}

GoogleSpreadsheetRiver.options = function(){
    return optionsObject;
};


GoogleSpreadsheetRiver.createRiver = function(){
    return assign(new Dispatcher(), new GoogleSpreadsheetRiver(optionsObject));
};




module.exports = GoogleSpreadsheetRiver;