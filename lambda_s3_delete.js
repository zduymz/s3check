const aws = require('aws-sdk');
const s3 = new aws.S3();
const R = require('ramda');
const config = require('./config');

const original_bucket = 'config.s3.source_bucket'
const copied_bucket = 'config.s3.dest_bucket'

const concatAll = R.unapply(R.reduce(R.concat, []))

const errHandler = (err) => {
    console.log('Error:', err)
}

const respHandler = (resp) => {
    console.log('Response:', resp)
}

const s3GetObjectVersions = (key) => {
    // problem: s3 list object use prefix, it can return more than 1 key
    return new Promise((resolve, reject) => {
        const params = {
            Bucket: config.s3.dest_bucket,
            Delimiter: '/',
            MaxKeys: 2000,
            Prefix: key,
        };
        s3.listObjectVersions(params, (err, data) => {
            if(err) reject(err)
            else {
                const kv = data.Versions.map(R.props(['Key', 'VersionId'])).filter(([k, v]) => k == key)
                resolve(kv)
            }
        })
    })
}

// @keys: [{Key: key1, VersionId: v1}]
const s3Delete = (keys) => {
    // delete all keys with one request
    const params = {
        Bucket: copied_bucket,
        Delete: {
            Objects: keys,
            Quiet: false
        }
    }
    return new Promise((resolve, reject) => {
            s3.deleteObject(params, (err, res) => {
                if (err) reject(err)
                else resolve(res)
            })
        })
}

const generateObjects = (data) => R.chain((x) => concatAll(x), data).map(R.zipObj(['Key', 'VersionId']))

const fake_payload = 'document10001032/0611/5b85/-c2b/c-4a/2f-b/cd0-/ffaf/5055/35cc~document10001032/088d/a78d/-3d4/9-48/0d-9/5a0-/b47a/1a7a/25c6~document10001032/0826/fcdb/-881/2-49/a9-9/e87-/f1a1/3bac/71d1~document10001032/08f3/b1ac/-b37/0-42/dc-a/b1d-/01ed/8a50/d5e2~document10001032/01d2/b43a/-b86/a-40/34-b/f3b-/eea3/7365/7c8b'
exports.handler = async (event) => {
    event.Records.forEach(function(record) {
        // Kinesis data is base64 encoded so decode here
        var payload = new Buffer(record.kinesis.data, 'base64').toString('utf-8');
        // each records contain at most 5 keys
        
        Promise.all(payload.split('~').map(s3GetObjectVersions))
            .then(generateObjects, errHandler)
            .then(s3Delete, errHandler)
            .then(respHandler, errHandler)
    });
};

function main(payload) {
    Promise.all(payload.split('~').map(s3GetObjectVersions)).then(generateObjects, errHandler).then(respHandler, errHandler)
}

main(fake_payload)
