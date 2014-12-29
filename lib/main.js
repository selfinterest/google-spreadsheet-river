/**
 * Created by: Terrence C. Watson
 * Date: 12/19/14
 * Time: 3:19 AM
 */

var Dispatcher = require('flux').Dispatcher;
var events = require("events");
var redis = require("redis");
var gsStreamFactory = require("google-spreadsheet-stream-reader").factory;
var inherits = require("util").inherits;
var jsesc = require("jsesc");
var _ = require("highland");
var RedisClient = require("node-redis-client");
var Q = require("q");

//We are an instance of event emitter
inherits(GoogleSpreadsheetRiver, events.EventEmitter);

var optionsObject = {
    expires: 86400000,         //one day, in milliseconds
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
        events.EventEmitter.call(this);
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


        this.keys = {};


    } else {
        return new GoogleSpreadsheetRiver(options);
    }

}


GoogleSpreadsheetRiver.prototype.synchronize = function(){
        if(this.synchronizing) return Q();
        this.expireAtInSeconds = Math.round((Date.now() + this.expires)/1000);
        this.synchronizing = true;
        //var expirationInSeconds = Math.round(this.expires / 1000);
        var count = 0, keys = {};
        var stream = this.gsReadStream.createStream();

        this.redisMulti.del(this.DBPrefix);
        this.keys = {};
        this.expireAt = Date.now() + this.expires;

        return Q.Promise(function(resolve, reject, notify){
            stream.on("data", function(){
                count++;
                //console.log(count);
            });

            stream.on("end", function(){

                var timestamp = Date.now();
                //console.log(timestamp);
                this.redisMulti.set(this.DBPrefix + "|date", timestamp);
                this.redisMulti.set(this.DBPrefix + "|expires", this.expires);
                this.redisMulti.set(this.DBPrefix + "|expireAt", this.expireAt);
                //Set expirations
                this.redisMulti.EXPIREAT(this.DBPrefix, this.expireAt);
                this.redisMulti.EXPIREAT(this.DBPrefix + "|date", this.expireAt);
                this.redisMulti.EXPIREAT(this.DBPrefix + "|expires", this.expireAt);

                this.redisMulti.exec(function(err, replies){
                    //console.log(replies);
                    this.keys = keys;

                    //console.log("Done: "+count);
                    resolve({count: count, timestamp: timestamp});
                }.bind(this));

            }.bind(this));

            _(stream).each(function(chunk){
                chunk = jsesc(chunk, {json: true});
                var obj = JSON.parse(chunk);
                var key = this.DBPrefix + ":" + obj[this.uniqueKey];
                //console.log(key);
                //console.log(chunk);
                //console.log(this.expires);

                this.redisMulti.set(key, chunk);
                this.redisMulti.EXPIREAT(key, this.expireAt);
                this.redisMulti.rpush(this.DBPrefix, key);      //we also push the key to a separate list.
                keys[key] = obj;
                this.emit("synchronized", {key: key, object: obj});
                return obj;
            }.bind(this));

        }.bind(this)).finally(function(){
            this.synchronizing = false;
        }.bind(this));

};

/**
 * Sets a flag in Redis
 * @param flag
 * @param value
 */
GoogleSpreadsheetRiver.prototype.setFlag = function(flag, value){

    var key = this.DBPrefix + "|" + flag;
    var redisClient = redis.createClient();
    return this.synchronizeOnlyIfNeeded().then(function(keys){
       return Q.Promise(function(resolve, reject, notify){
           redisClient.get(this.DBPrefix + "|expireAt", function(err, expireAt){
                if(err){
                    reject(err);
                } else {
                    var multi = redisClient.multi();
                    multi.set(key, value);
                    multi.EXPIREAT(key, expireAt);
                    multi.exec(function(err, replies){
                        if(err){
                            reject(err);
                        } else {
                            resolve(replies);
                        }
                    })
                }
           }.bind(this));
       }.bind(this));
    }.bind(this));

}

GoogleSpreadsheetRiver.prototype.getFlags = function(flags){
    return this.synchronizeOnlyIfNeeded().then(function(keys){
       var promises = [];
       promises = flags.map(function(flag){
           var key = this.DBPrefix + "|" + flag;
           return Q.Promise(function(resolve, reject, notify){
               this.redisClient.get(key, function(err, value){
                   if(err){
                       reject(err);
                   } else {
                       resolve({flag: flag, value: value});
                   }
               }.bind(this));
           }.bind(this));
       }.bind(this));
       return Q.all(promises).then(function(flagsAndValues){
          //Convert to object
          return flagsAndValues.reduce(function(resultObj, flagAndValue){
              resultObj[flagAndValue.flag] = resultObj[flagAndValue.value];
          }, {});
       });
    }.bind(this));
}
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

GoogleSpreadsheetRiver.prototype.synchronizeOnlyIfNeeded = function(){
    var keyPromise = this.getKeys();
    var lastSynchronized = this.getLastSynchronized();

    return Q.all([keyPromise, lastSynchronized]).spread(function(keys, time){
        //If keys is an empty array, or time wasn't set.. we need to synchronize
        if(keys.length == 0 || !time) {
            return this.synchronize().then(function(){
                return this.getKeys();
            }.bind(this));
        } else {
            return keys;
        }
    }.bind(this))
};
/**
 * Updates the redis store. This is different from a full synchronization because the module only processes entries it doesn't already have.
 */
GoogleSpreadsheetRiver.prototype.update = function(){

    return this.getKeys().then(function(keys){
            var synchronize = Q(keys);
            if(!keys){  //If we don't have the keys, synchronize, then get keys
                synchronize = this.synchronize().then(this.getKeys());
            }

            return synchronize;
        }.bind(this))
    .then(function(keys){
        //Make a huuuuge query condition with all these keys
        var regex = new RegExp("^" + this.DBPrefix + ":(\\w+)$"), uniqueKeyValue;
        var query = keys.reduce(function(accumulatedQuery, queryPart){
            var matches = queryPart.match(regex);
            uniqueKeyValue = matches;

            if(matches){
                uniqueKeyValue = matches[1];        //i.e. the csv_id or the like
                if(accumulatedQuery) {
                    accumulatedQuery += " and " + this.uniqueKey + " != " + uniqueKeyValue;
                } else {
                    accumulatedQuery += this.uniqueKey + " != " + uniqueKeyValue;
                }
            }


            return accumulatedQuery;
        }.bind(this), "");
        var stream = gsStreamFactory
            .email(this.email)
            .keyFile(this.keyFile)
            .spreadsheetName(this.spreadsheetName)
            .worksheetName(this.worksheetName)
            .https(this.https)
            .query(query).createStream();

            var redisMulti = this.redisClient.multi();      //we need a separate multi

            _(stream).each(function(chunk){
                chunk = jsesc(chunk, {json: true});
                var obj = JSON.parse(chunk);
                var key = this.DBPrefix + ":" + obj[this.uniqueKey];


                redisMulti.setex(key, expirationInSeconds, chunk);
                redisMulti.rpush(this.DBPrefix, key);      //we also push the key to a separate list.
                keys[key] = obj;
                this.emit("updated", {key: key, object: obj});
                return obj;
            }.bind(this));
        }.bind(this));
};


/**
 * @returns {*}
 */

GoogleSpreadsheetRiver.prototype.getAll = function(forceRefresh){

    var promise;

    //TODO Fix this
    /*if(!forceRefresh){
        promise = this.synchronizeOnlyIfNeeded();
    } else {
        promise = this.synchronize();
    }*/

    promise = this.synchronizeOnlyIfNeeded();

    return promise.then(function(keys){
        return Q.Promise(function(resolve, reject, notify){
            this.redisClient.mget(keys, function(err, result){
               if(err) {
                   reject(err);
               } else {
                   resolve({keys: keys, values: result});
               }
            });
        }.bind(this));
    }.bind(this));
}

GoogleSpreadsheetRiver.prototype.createStream = function(){
    return _(this.getAll());
}

GoogleSpreadsheetRiver.prototype.getExpiration = function(){
    return Q.Promise(function(resolve, reject){
        this.redisClient.get(this.DBPrefix + "|expires", function(err, reply){
            if(err) {
                return reject(error);
            } else {
                return resolve(parseInt(reply));
            }
        });
    }.bind(this));
}

GoogleSpreadsheetRiver.prototype.isExpired = function(){
    return Q.Promise(function(resolve, reject){
       var lastSync = this.getLastSynchronized();
       var expires = this.getExpiration();
    }.bind(this));
};

GoogleSpreadsheetRiver.prototype.getNumberOfKeys = function(){
    return Q.Promise(function(resolve, reject){
        this.redisClient.llen(this.DBPrefix, function(err, reply){
           if(err) return reject(err);
           resolve(reply);
        });
    }.bind(this));
};

GoogleSpreadsheetRiver.prototype.getKeys = function(){
    return Q.Promise(function(resolve, reject){
       this.redisClient.lrange(this.DBPrefix, 0, -1, function(err, result){
          if(err) return reject(err);
           result = jsesc(result, {json: true});
           resolve(JSON.parse(result));
       });
    }.bind(this));
};

GoogleSpreadsheetRiver.prototype.deleteAllKeys = function(){
    return Q.nfcall(this.redisClient.del, this.DBPrefix);
};

/**
 * Gets and returns an item by key
 */
GoogleSpreadsheetRiver.prototype.get = function(key) {
    return Q.Promise(function(resolve, reject, notify){
        if(!key) return resolve(null);

        if(this.keys[key]) {
            //First, is expiresAt set at all?
            if(this.expiresAt && Date.now <= this.expiresAt) {
                return resolve(this.keys[key]);
            }
        }
        //Key not in memory. Try redis.
        this.redisClient.get(key, function(err, result){
           if(err) {
               reject(err);
           } else {
               if(result){
                   resolve(JSON.parse(result));
               } else {
                   //K, now try Google
                   //Generate a query
                   var regex = new RegExp("^" + this.DBPrefix + ":(\\w+)$");
                   var matches = key.match(regex), queryValue;
                   if(matches){
                       queryValue = this.uniqueKey + " = " + matches[1];
                       var gsReadStream = gsStreamFactory
                               .email(this.email)
                               .keyFile(this.keyFile)
                               .spreadsheetName(this.spreadsheetName)
                               .worksheetName(this.worksheetName)
                               .https(this.https)
                               .query(queryValue).createStream();
                       _(gsReadStream).each(function(value){
                           this.keys[key] = value;
                           //Now that we have the value, add it to both redis and our internal key store
                           value = jsesc(value, {json: true});
                           this.redisClient.set(key, value);

                           resolve(value);
                       }.bind(this));
                       //.limit(50)

                   }

               }
           }
        }.bind(this));
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