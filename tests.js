'use strict';

let aws  = require('aws-sdk');
let chai = require('chai');


let config = { "accessKeyId": "none", "secretAccessKey": "none", "region": "localhost" , "endpoint" : new aws.Endpoint('http://localhost:8000') };
aws.config.update(config);

let dynamoTableCache = require('./index');

let cache = new dynamoTableCache('users', { indexes : ['userName', 'email'], expire : 1} , {dynamoDbOptions : { apiVersion: '2012-08-10'} });

describe('test dynamoTableCache object', function () {

    it('should be return  object', function () {
        chai.expect(cache).to.be.an('object');
    });

    it('should be an instance of dynamoTableCache class', function () {
        chai.expect(cache).to.be.an.instanceOf(Object);
    });

    it('should return a promise', function () {
       chai.expect(cache.fill()).to.be.an('Promise');
    });

    it('promise should be resolved', function (done) {
        cache.fill().then(function () {
            done();
        });
    });

    it('should be return from cache', function (done) {
        cache.fill().then(function () {

            cache.get('akaki.kherkeladze@gmail.com', 'email').then(function (data) {
                chai.expect(data).to.be.an('Object');
                done();
            });

        });

    });
});
