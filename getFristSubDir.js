const aws = require('aws-sdk')
const R = require('ramda')
const fs = require('fs')
const Redis = require('ioredis')
const redis = new Redis()
const pipeline = redis.pipeline()
const config = require('./config')
const s3 = new aws.S3()

// 10000265/1d7b/628f/-0c3/1-41/fe-b/3c1-/3d07/7fd2/5104
// level 1   2     3   4     5   6   7    8    9

const getFirstSubDirectory = (bucket, filename, prefix) => {
    let result = []
    const opts = { 
        Bucket: bucket,
        Delimiter: '/'
    }
    if (prefix) opts.Prefix = prefix

    const writeToFile = (data) => {
        fs.appendFile(filename, data, (err) => { if (err) console.log(err) });
    }

    const callback = (err, data) => {
        console.log(data)
        const lines = R.join('\n', data.CommonPrefixes.map(R.prop('Prefix')))
        writeToFile(lines)
        if(data.IsTruncated) {
           getObject(data.NextContinuationToken)
        }
    }

    const getObject = (token) => {
        if (token) {
            opts.ContinuationToken = token
        }
        s3.listObjectsV2(opts, callback)    
    }

    getObject()
}


const original_bucket = config.s3.source_bucket
const copied_bucket = config.s3.dest_bucket
getFirstSubDirectory(original_bucket, 'level1.txt')
