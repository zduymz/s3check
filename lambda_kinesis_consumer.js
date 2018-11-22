const aws = require('aws-sdk');
const config = require('./config')
const s3 = new aws.S3();

const original_bucket = config.s3.source_bucket
const copied_bucket = config.s3.dest_bucket

const errHandler = (err) => {
    console.log('Error:', err)
}

const respHandler = (resp) => {
    console.log('Response:', resp)
}
const s3copy = (params) => {
    return new Promise((resolve, reject) => {
            s3.copyObject(params, (err, res) => {
                if (err) reject(err)
                else resolve(res)
            })
        })
}

const s3copy2 = async (params) => {
    return s3.copyObject(params, (err, res) => {
                if (err) errHandler(err)
                else respHandler(res)
            })
}

exports.handler = (event) => {
    // TODO implement
    event.Records.forEach(function(record) {
        // Kinesis data is base64 encoded so decode here
        var payload = new Buffer(record.kinesis.data, 'base64').toString('utf-8');
        const params = {
            Bucket: copied_bucket, 
            CopySource: `/${original_bucket}/${payload}`, 
            Key: payload
        };
        console.log('Decoded payload:', params);
        return s3copy(params).then(respHandler, errHandler)
    });
};

