const aws = require('aws-sdk');
const s3 = new aws.S3();
const R = require('ramda');
const config = require('./config');

const copied_bucket = config.s3.dest_bucket;

const concatAll = R.unapply(R.reduce(R.concat, []));

const sleep = (ms) => {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
};

const getRandomTimeout = () => Math.floor(Math.random() * 5000);

const errHandler = (err) => {
    console.log('Error:', err);
};

const respHandler = (resp) => {
    if(resp.Errors.length > 0) {
        console.log('Response:', resp.Errors);
    } else {
        console.log('Done');
    }
};

const s3GetObjectVersions = (key) => {
    // problem: s3 list object use prefix, it can return more than 1 key
    return new Promise((resolve, reject) => {
        const listVersions = async (key) => {
            const params = {
                Bucket: copied_bucket,
                Delimiter: '/',
                MaxKeys: 2000,
                Prefix: key,
            };
            // avoid flooding s3 api, if it work, no error "SlowDown: Please reduce your request rate"
            await sleep(getRandomTimeout(), s3.listObjectVersions(params, (err, data) => {
                if(err) {
                    console.log('s3GetObjectError', err);
                    return sleep(getRandomTimeout(), listVersions(key));
                } else {
                    // if key does not exist, [] is returned
                    const kv = data.Versions.map(R.props(['Key', 'VersionId'])).filter(([k, v]) => k == key);
                    kv.length > 0 ? resolve(kv) : reject(kv);
                    //kv.length > 0 ? resolve(kv) : console.log(`${key} deleted`)
                }
            }));
        };
        // keep asking s3 api when a result returned
        listVersions(key);
    }).catch(() => []);
    // When any of function called was rejected. Promise.all will stop
    // it mean when any key in batch deleted, the rest will be ignored
    // .catch() will solve this problem
};

// @keys: [{Key: key1, VersionId: v1}]
const s3Delete = (keys) => {
    // delete all keys with one request
    return new Promise((resolve, reject) => {
        if (keys.length == 0) return resolve({Errors: []});
        const handlerDelete = async (keys) => {
            const params = {
                Bucket: copied_bucket,
                Delete: {
                    Objects: keys,
                    Quiet: false
                }
            };
            // avoid flooding s3 api, error limit rate, damn it!
            await s3.deleteObjects(params, (err, res) => {
                if (err) {
                    console.log('s3DeleteObject', err);
                    return sleep(getRandomTimeout(), handlerDelete(keys));
                }
                else resolve(res);
            });
        };
        // s3 deleteObjects can not handle over 1000 keys at one time
        R.splitEvery(1000, keys).map(handlerDelete);
    });
};

const generateObjects = (data) => R.chain((x) => concatAll(x), data).map(R.zipObj(['Key', 'VersionId']));

/*
 * Follwing export is used for aws lambda function
 */

exports.handler = (event) => {
    event.Records.forEach(function(record) {
        // Kinesis data is base64 encoded so decode here
        var payload = new Buffer(record.kinesis.data, 'base64').toString('utf-8');
        console.log('Payload:', payload);
        Promise.all(payload.split('~').map(s3GetObjectVersions))
            .then(generateObjects)
            .then(s3Delete)
            .then(respHandler);
    });
};


/*
 * Following export is use for running on local machine
 */
exports.worker = (payload) => {
    console.log(payload)
}
