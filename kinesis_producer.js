const Redis = require('ioredis')
const aws = require('aws-sdk')
const R = require('ramda')
const util = require('util')
const config = require('./config')
const program = require('commander')

/*
 * When the process check S3 key and Redis key finished, there are keys left in Redis.
 * Need to dump all those keys to Kinesis, there is another aws lambda task will 
 * handle those keys
 */

const redis = new Redis()
// Helper
const removePrefix = (x) => x.substring(x.indexOf('/'))
const removeSplash = (x) => x.replace(/\//g,'')
const getKey = (x) => x.Key
const concatAll = R.unapply(R.reduce(R.concat, []))
const get4Byte = (start) => (x) => x.substring(start, start + 4)
const getLastPart = (x) => x.substring(32)
const getOriginalS3Key = (xs) => {
        // input: bl959fe89c-66eb-4c76-8420-5bdc8c0e0e14::jpg::300::142
        // output: prefix/bl95/9fe8/9c-6/6eb-/4c76/-842/0-5b/dc8c/0e0e14::jpg::300::142
        const x = removePrefix(xs)
        const prefix = x.substring(0, x.indexOf('/'))
        return R.join('/', [prefix, get4Byte(0)(x), get4Byte(4)(x), get4Byte(8)(x), get4Byte(12)(x), get4Byte(16)(x), get4Byte(20)(x), get4Byte(24)(x), get4Byte(28)(x), getLastPart(x)])
    }

//shard id format: shardId-000000000001
const generateShardId = (x) => `shardId-${x.toString().padStart(12, '0')}`

// Declare
const kinesis_streamname = config.kinesis.stream
const kinesis = new aws.Kinesis({region: config.kinesis.region})
const kinesis_shards = R.range(1, config.kinesis.shards + 1).map(generateShardId)

const dumpRedis = () => {
    const stream = redis.scanStream({
        count: config.kinesis.redis_count
        match: `${s3_prefix}*`
    })
    const draftDeleteCommand = (x) => concatAll(['del'], [x])
    const removeRedisKey = (xs) => {
        const tmp = xs.map(draftDeleteCommand)
        console.log(xs)
        redis.pipeline(tmp).exec((err, res) => { })
            .then(() => console.log(`Removed ${xs.length} keys`))
    }
    stream.on('data', (data) => {
        stream.pause()
        // publish keys to kinesis
        kinesis_producer(data).then(() => {
            console.log('Resume Redis Scan')
            stream.resume()
        })
    })

    stream.on('end', () => {
        console.log(`Dumped ${no} keys`)
        // TODO: flush database
        redis.disconnect()
    })
}

const kinesis_test = () => {
    const params = {
        StreamName: 'dmai-s3',
        Limit: 300
    }
    kinesis.describeStream(params, (err, data) => {
        console.log(err, data.StreamDescription.Shards.length)
    })
}

const kinesis_producer = (data) => {
    // draft message
    const generateRecord = (v, i) => ({ Data: getOriginalS3Key(v), PartitionKey: kinesis_shards[i%200] })

    const params = {
        Records: data.map(generateRecord),
        StreamName: kinesis_streamname
    }
    const msgCount = data.length
    const handleError = (err) => { console.log('Kinesis Error', err) }
    const handleResponse = (res) => {
        console.log(`Sent: ${msgCount} Failed: ${res.FailedRecordCount}`)
        // if something go wrong, will push all data again
        if (res.FailedRecordCount > 0) {
            return kinesis_producer(data)
        }
        return null
    }

    const producerP = new Promise(function(resolve, reject) {
        kinesis.putRecords(params, (err, res) => {
            err == null ? resolve(res) : reject(err)
        })
    })
    return producerP.then(handleResponse, handleError)
}

program.option('-p, --prefix <required>', 'S3 bucket in first level')
    .parse(process.argv)
const s3_prefix = program.prefix || null

