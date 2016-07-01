'use strict';

let cache       = require('lambda-local-cache');
let aws         = require('aws-sdk');


class DynamoTableCache {

    /**
     *
     * @param {String} table (required)
     * @param {Object} options (required) {
     *  expire: default expiry time span in seconds
     *  fillIfEmpty - fill cache from database on first request
     *  ifNotFound ('fill' or 'get') - specify what to do if object not found in cache
     *  indexes: [ array of index fields ] - first index name is treated as primary index
     * }
     * @param {Object=} awsConfig (optional) - aws configuration parameters
     */
    constructor(table, options, awsConfig) {
        if (!awsConfig) awsConfig = {};
        if (!table) throw new Error('table name must be provided');
        if (!options.indexes.length) throw new Error('indexes must be provided');
        
        this._table        = table;
        this._options      =  options;
        this._primaryIndex = options.indexes[0];
        this._cache        = new cache(table, options);
        this._dbc          = new aws.DynamoDB.DocumentClient(awsConfig);
    }

    fill() {
        let params = { TableName : this._table };

        return new Promise(resolve => {
            this._dbc.scan(params, (err, data) => {
                if(err) throw err;

                this._cache.set(data.Items, this._options.expire);
                console.log(this._table + ' table fetched successfully');
                resolve();
            });
        });
    }

    /**
     * @param {String} key (required)
     * @param {String=} indexName (optional) - default is primary index
     * */
    _getFromCache(key, indexName) {
        return this._cache.get(key, indexName);
    }

    _getFromDB(key, indexName) {
        return new Promise((resolve, reject) => {
            indexName = indexName || this._primaryIndex;
            let params = {

                TableName: this._table,
                KeyConditionExpression: indexName + " = :hkey",
                ExpressionAttributeValues: {
                    ':hkey': key
                }
            };

            this._dbc.query(params, (err, data) => {
                if(err) return reject(err);
                else resolve(data.Items[0]);
                console.log('get from DB');
            }); 
        });
    }

    /**
     * @param {String} key (required)
     * @param {String} indexName (required) - default is primary index
     * */
    get(key, indexName) {

        let doGet = () => {
            return new Promise((resolve, reject) => {
                let result = this._getFromCache(key, indexName);
                if (result) {
                    console.log('get from cache');
                    return resolve(result);
                }

                //decide what to do if record not found in cahce
                switch (this._options.ifNotFound) {
                    case 'fill': //fill cache from DB
                        return this.fill().then(() => {
                            return resolve(this._getFromCache(key, indexName));
                        });

                    case 'get': //get single record from DB
                        return this._getFromDB(key, indexName).then(data => {
                            if(data) this._cache.set(data, this._options.expire);
                            resolve(data);
                        }).catch(reject);

                    default: //just return empty result
                        return resolve(result);
                }
            });
        };

        //decide if we need to fill empty cahe before geting record
        return this._options.fillIfEmpty && !this._cache.count ?
            this.fill().then(doGet) : doGet();

    }

    /**
     * @param {String} key (required)
     * @param {String} indexName (required)
     * */
    remove(key, indexName) {
        this._cache.remove(key, indexName);
        console.log('removed...')
    }

    /**
     * clear all cached records
     * */
    clear() {
        this._cache.clear();
        console.log('clearing cache...');

    }
}

module.exports = DynamoTableCache;