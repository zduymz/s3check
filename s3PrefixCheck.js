const aws = require('aws-sdk')
const R = require('ramda')
const fs = require('fs')
const async = require('async')
const Redis = require('ioredis')
const util = require('util')
const program = require('commander')

const config = require('./config')
const redis = new Redis()
const s3 = new aws.S3()

// 10000265/1d7b/628f/-0c3/1-41/fe-b/3c1-/3d07/7fd2/5104
const removePrefix = (x) => x.substring(x.indexOf('/'))
const removeSplash = (x) => x.replace(/\//g,'')
const getKey = (x) => x.Key
const concatAll = R.unapply(R.reduce(R.concat, []))
const get4Byte = (start) => (x) => x.substring(start, start + 4)
const getLastPart = (x) => x.substring(32)
const getOriginalS3Key = (xs) => {
        // input: prefix/bl959fe89c-66eb-4c76-8420-5bdc8c0e0e14::jpg::300::142
        // output: prefix/bl95/9fe8/9c-6/6eb-/4c76/-842/0-5b/dc8c/0e0e14::jpg::300::142
        const x = removePrefix(xs)
        const prefix = x.substring(0, x.indexOf('/'))
        return {Key: R.join('/', [ prefix, get4Byte(0)(x), get4Byte(4)(x), get4Byte(8)(x), get4Byte(12)(x), get4Byte(16)(x), get4Byte(20)(x), get4Byte(24)(x), get4Byte(28)(x), getLastPart(x)])}
    }

/*
 * @bucket: bucket name
 * @prefix: first level sub dir in bucket
 * @mode: 0: write to redis
 *        1: read/dele from redis
 */
const runCheck = (bucket, prefix, mode) => {
    const opts = {
        Bucket: bucket,
        Prefix: prefix
    }

    const draftSetCommand = (x) => concatAll(['set'], [x], [''])
    const draftGetCommand = (x) => concatAll(['get'], [x])
    const writeToFile = (filename, xs) => {
        return xs.length > 0 ? fs.appendFile(filename, R.join('\n', R.concat(xs, [''])), (err) => {
            if (err) console.log('error write to file: ', err)
        }) : null
    }

    const filename = removeSplash(prefix) + '.txt'
    const cbWriteRedis = (err, res) => err ? console.log('error: ', err) : null

    const writeToRedis = (input) => {
        const [data, nextToken] = input
        const xs = data.map(draftSetCommand)
        return redis.pipeline(xs).exec(cbWriteRedis)
            .then(() => console.log(`Added ${data.length} keys`))
            .then(() => nextToken)
    }

    const removeKeyS3 = (data) => {
        // maximum objects is 1000 per call
        // TODO: delete multiple versions
         var params = {
            Bucket: bucket,
            Delete: {
                Objects: data.map(getOriginalS3Key),
                Quiet: false
            }
        }
        console.log(params, data.map(getOriginalS3Key))
        return data.length !== 0 ? s3.deleteObjects(params, (err, res) => {
            if(err) console.log('removeKeyS3 Error:', err)
            console.log(`deleted ${data.length}`)
        }) : null
    }

    const deleteKeyRedis = async (input) => {
        const [data, nextToken] = input
        // size can not over 1000
        let tmp = []
        let i = 0
        const remove = async (k) => {
            const cb = function(err, res) {
                // 0: there is no key to delete
                if(res == 0) {
                    // TODO: write to file one by one line or batch
                    // writeToFile(k + '\n')
                    tmp.push(k)
                }
            }
            await redis.del(k, cb)
        }
        // return Promise.all(data.map(remove)).then(() => removeKeyS3(tmp))
        return Promise.all(data.map(remove)).then(() => writeToFile(filename, tmp))
            .then(() => nextToken)
    }

    const handleS3Object = (data) => {
        const addPrefix = (x) => removeSplash(prefix) + `/${x}`
        const lines = data.Contents.map(R.compose(addPrefix, removeSplash, removePrefix, getKey))
        return [lines, data.IsTruncated ? data.NextContinuationToken : null]
    }

    const getS3Object = (token) => {
        if (token) {
            opts.ContinuationToken = token
        }

        return new Promise(function(resolve, reject) {
            s3.listObjectsV2(opts, (err, data) => {
                err == true ? reject(err) : resolve(data)
            })
        })
    }
    const errorhandler = (err) => {console.log(err)}
    const finNotify = (msg) => {
        console.log(msg)
        redis.disconnect()
    }

    // adding mean write to redis
    const adding = (t) => getS3Object(t)
                        .then(handleS3Object, errorhandler)
                        .then(writeToRedis, errorhandler)
                        .then((nextToken) => {
                            if(nextToken) adding(nextToken)
                            else finNotify('Adding done')
                        }, errorhandler)

    // subtract mean check redis, and delete s3 if not exist
    const subtract = (token) => getS3Object(token)
                                .then(handleS3Object, errorhandler)
                                .then(deleteKeyRedis, errorhandler)
                                .then((nextToken) => {
                                    if(nextToken) subtract(nextToken)
                                    else finNotify('Subtracting done')
                                }, errorhandler)

    return mode === 0 ? adding(null) : subtract(null)
}

const original_bucket = config.s3.source_bucket
const copied_bucket = config.s3.dest_bucket

program.option('-p, --prefix <required>', 'S3 bucket in first level')
    .option('-m, --mode <required>', '0 or 1')
    .parse(process.argv)

const prefix = program.prefix || null
const mode = parseInt(program.mode) || 0
const bucket = (mode === 0) ? original_bucket : copied_bucket

if (prefix == null) {
    redis.disconnect()
    process.exit(1)
}
runCheck(bucket, prefix, mode)

// const objectid = 'document10001032/blfb/230c/89-7/661-/48df/-b11/6-b1/69d0/'
// var params = {
//   Bucket: copied_bucket,
//   Delimiter: '/',
//   MaxKeys: 2000,
//   Prefix: objectid
// };


// s3.listObjectVersions(params, (err, data) => console.log(data))
