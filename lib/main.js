/**
 * Created by: Terrence C. Watson
 * Date: 12/19/14
 * Time: 3:19 AM
 */

var Dispatcher = require('flux').Dispatcher;
var assign = require("object-assign");
var redis = require("redis");
var gsStreamFactory = require("google-spreadsheet-stream-reader").factory;
var inherits = require("util").inherits;
var jsesc = require("jsesc");
var _ = require("highland");
var RedisClient = require("node-redis-client");
var Q = require("q");

inherits(GoogleSpreadsheetRiver, Dispatcher);

var optionsObject = {
    expires: 86400,         //one day
    email: null,
    keyFile: null,
    spreadsheetName: null,
    worksheetName: null,
    https: true,
    uniqueKey: "_id",
    DBPrefix: "gss_river",
    DBPort: 6379,
    DBHost: "localhost"
};

function GoogleSpreadsheetRiver(options){
    if(this instanceof GoogleSpreadsheetRiver){
        Dispatcher.call(this);
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
            //.limit(50)
        ;

        //Create a redis stream
        this.redisClient = redis.createClient();

        this.redisMulti = this.redisClient.multi();




    } else {
        return new GoogleSpreadsheetRiver(options);
    }

}


GoogleSpreadsheetRiver.prototype.synchronize = function(){
        var count = 0;
        var stream = this.gsReadStream.createStream();

        return Q.Promise(function(resolve, reject, notify){
            stream.on("data", function(){
                count++;
                //console.log(count);
            });

            stream.on("end", function(){
                var timestamp = Date.now();
                //console.log(timestamp);
                this.redisMulti.set(this.DBPrefix + "|date", timestamp);
                this.redisMulti.exec(function(){

                    //console.log("Done: "+count);
                    resolve({count: count, timestamp: timestamp});
                });

            }.bind(this));

            _(stream).each(function(chunk){
                chunk = jsesc(chunk, {json: true});
                var obj = JSON.parse(chunk);
                var key = this.DBPrefix + ":" + obj[this.uniqueKey];
                this.redisMulti.set(key, chunk, this.expires);
                this.redisMulti.rpush(this.DBPrefix, key);      //we also push the key to a separate list.

                return obj;
            }.bind(this));

        }.bind(this));


};

GoogleSpreadsheetRiver.prototype.getLastSynchronized = function(){
    return Q.Promise(function(resolve, reject){
        this.redisClient.get(this.DBPrefix + "|date", function(err, reply){
           if(err) {
               return reject(error);
           } else {
               return resolve(parseInt(reply));
           }
        });
    }.bind(this));

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
    return new GoogleSpreadsheetRiver(optionsObject);

    //return assign(new Dispatcher(), new GoogleSpreadsheetRiver(optionsObject));
};




module.exports = GoogleSpreadsheetRiver;