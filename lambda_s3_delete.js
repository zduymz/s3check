const aws = require('aws-sdk');
const s3 = new aws.S3();
const R = require('ramda');
const config = require('./config');

const copied_bucket = config.s3.dest_bucket;

const concatAll = R.unapply(R.reduce(R.concat, []));

const errHandler = (err) => {
    console.log('Error:', err);
};

const respHandler = (resp) => {
    if(resp.Errors.length > 0) {
        console.log('Response:', resp.Errors);
    } else {
        console.log('Done')
    }
};

const s3GetObjectVersions = (key) => {
    // problem: s3 list object use prefix, it can return more than 1 key
    return new Promise((resolve, reject) => {
        const params = {
            Bucket: copied_bucket,
            Delimiter: '/',
            MaxKeys: 2000,
            Prefix: key,
        };
        s3.listObjectVersions(params, (err, data) => {
            if(err) reject(err);
            else {
                const kv = data.Versions.map(R.props(['Key', 'VersionId'])).filter(([k, v]) => k == key);
                resolve(kv);
            }
        });
    });
};

// @keys: [{Key: key1, VersionId: v1}]
const s3Delete = (keys) => {
    // delete all keys with one request
    const params = {
        Bucket: copied_bucket,
        Delete: {
            Objects: keys,
            Quiet: false
        }
    };
    return new Promise((resolve, reject) => {
            s3.deleteObjects(params, (err, res) => {
                if (err) reject(err);
                else resolve(res);
            });
        });
};

const generateObjects = (data) => R.chain((x) => concatAll(x), data).map(R.zipObj(['Key', 'VersionId']));

exports.handler = (event) => {
    event.Records.forEach(function(record) {
        // Kinesis data is base64 encoded so decode here
        var payload = new Buffer(record.kinesis.data, 'base64').toString('utf-8');
        console.log('Payload:', payload);
        Promise.all(payload.split('~').map(s3GetObjectVersions))
            .then(generateObjects, errHandler)
            .then(s3Delete, errHandler)
            .then(respHandler, errHandler);
    });
};