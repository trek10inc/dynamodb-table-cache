'use strict';

let cache       = require('lambda-local-cache');
let aws         = require('aws-sdk');


class DynamoTableCache {

    /**
     *
     * @param {String} table (required)
     * @param {Object} options (required) {
     *  expire: default expiry time span in seconds
     *  indexes: [ array of index fields ] - first index name is treated as primary index
     * }
     * @param {Object=} awsConfig (optional) - aws configuration parameters
     */
    constructor(table, options, awsConfig) {
        if(!table) throw new Error('table name must be provided');
        if(!options.indexes.length) throw new Error('indexes must be provided');

        if(!awsConfig) awsConfig = {};
        this._dbc = new aws.DynamoDB.DocumentClient(awsConfig);
        this._table = table;
        this._options =  options;
        this._cache = new cache(table , options);
    }

    fill() {
        let params = { TableName : this._table };

        return new Promise(resolve => {
            this._dbc.scan(params, (err, data) => {
                if(err) throw err;

                data.Items.forEach((value) => this._cache.set(value, this._options.expire)); //saves fetched items into local cache
                resolve();
            });
            console.log(this._table + ' table fetched successfully');
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

        return new Promise(resolve => {
            let params = {

                TableName: this._table,
                KeyConditionExpression: indexName + " = :hkey",
                ExpressionAttributeValues: {
                    ':hkey': key
                }
            };

            this._dbc.query(params, (err, data) => {
                if(err) throw new Error(err);
                resolve(data.Item[0] || null);
            }); 
        });

    }


    /**
     * @param {String} key (required)
     * @param {String} indexName (required) - default is primary index
     * */
    get(key, indexName) {
        return new Promise((resolve, reject) => {
            let result = this._getFromCache(key, indexName);

            if (result) resolve(result);
            else {
                this._getFromDB(key, indexName).then(data => {

                    if(data) {
                        this._cache.set(data, this._options.expire);
                        resolve(data);
                    }
                    else reject(null);
                });
            }
        });
    }

    /**
     * @param {String} key (required)
     * @param {String} indexName (required)
     * */
    remove(key, indexName) {
        this._cache.remove(key, indexName);
    }

    clear() {
        this._cache.clear();
    }
}

module.exports = DynamoTableCache;