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
var through2 = require("through2");
var jsesc = require("jsesc");
var _ = require("highland");
var RedisClient = require("node-redis-client");

inherits(GoogleSpreadsheetRiver, Dispatcher);

var optionsObject = {
    expires: Date.now() + 3600000,
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
        //this.redisClient = redis.createClient();
        //this.redisClient = this.redisClient.multi();
        
        this.redisClient = new RedisClient();


        //Create the transformer
        var my = this;
        this.jsonToRedisTransformer = through2.obj(function(chunk, enc, callback){
            chunk = jsesc(chunk, {quotes: 'double'});

            // /Is chunk valid JSON?
            try {
                var obj = JSON.parse(chunk);
                var key = my.DBPrefix + ":" + obj[my.uniqueKey];
                console.log(key);
                my.redisClient.call('SETEX', key, chunk, this.expires, function(){
                  this.push(chunk);
                  callback();
                }.bind(this));


                //my.redisClient.set(key, chunk);
                //console.log(chunk);
                //this.push(chunk);
            } catch (e){
                console.log(e);
                //Just ignore the chunk.
            } finally {
                //Check the back pressure. If we've got more than 12 things, then stop.
                /*if(my.redisClient.command_queue.length < 13){
                    callback();
                }  else {
                    console.log("Getting back pressure. So stopping for now.");
                    my.redisClient.once("drain", function(){
                       callback();
                    });
                }*/
                callback();

            }
        });

        this.jsonToRedisTransformer.on("error", function(){
            console.log(arguments);
        })

    } else {
        return new GoogleSpreadsheetRiver(options);
    }

}


GoogleSpreadsheetRiver.prototype.synchronize = function(){
        var count = 0;
        var stream = this.gsReadStream.createStream();

        stream.on("data", function(){
            count++;
            //console.log(count);
        });

        stream.on("end", function(){
            console.log("Done!");
        });

        this.redisClient.on("connect", function(){
            _(stream).each(function(chunk){
                chunk = jsesc(chunk, {json: true});
                var obj = JSON.parse(chunk);
                var key = this.DBPrefix + ":" + obj[this.uniqueKey];
                this.redisClient.call('SET', key, chunk);
                return obj;
            }.bind(this));


        }.bind(this));


        this.jsonToRedisTransformer.on("end", function(){
            console.log("Done!");
        });

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