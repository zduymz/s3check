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
        const x = removePrefix(xs).replace('/','')
        const prefix = xs.substring(0, xs.indexOf('/'))
        return R.join('/', [prefix, get4Byte(0)(x), get4Byte(4)(x), get4Byte(8)(x), get4Byte(12)(x), get4Byte(16)(x), get4Byte(20)(x), get4Byte(24)(x), get4Byte(28)(x), getLastPart(x)])
    }

//shard id format: shardId-000000000001
const generateShardId = (x) => `shardId-${x.toString().padStart(12, '0')}`

// Declare
const kinesis_streamname = config.kinesis.copy.stream
const kinesis = new aws.Kinesis({region: config.kinesis.copy.region})
const kinesis_shards_count = config.kinesis.copy.shards
const kinesis_shards = R.range(1, config.kinesis.copy.shards + 1).map(generateShardId)

const counter = () => {
  let count = 0
  const total = () => count
  const reset = (x) => count = x
  const update = (x) => count += (x) ? x : 0
  const timesleep = () => 2**count
  return { update: update, total: total, timesleep: timesleep, reset: reset }
}

function sleep(ms){
    return new Promise(resolve => {
        setTimeout(resolve, ms)
    })
}

const dumpRedis = () => {
    const stream = redis.scanStream({
        count: config.kinesis.copy.redis_count,
        match: `${s3_prefix}*`
    })
    const draftDeleteCommand = (x) => concatAll(['del'], [x])
    // const removeRedisKey = (xs) => {
    //     const tmp = xs.map(draftDeleteCommand)
    //     console.log(xs)
    //     redis.pipeline(tmp).exec((err, res) => { })
    //         .then(() => console.log(`Removed ${xs.length} keys`))
    // }
    const count = counter()
    stream.on('data', (data) => {
        if(data.length > 0) {
            count.update(data.length)
            stream.pause()
            // publish keys to kinesis
            kinesis_producer(data).then(() => {
                console.log('Resume Redis Scan')
                sleep(50).then(stream.resume())
            })
        }
    })

    stream.on('end', () => {
        // Bc async push msg to kinesis, you will see this output earlier a little bit
        console.log(`Dumped ${count.total()} keys`)
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
    // draft message, 5 keys into a record
    const generateRecord = (xs, i) => ({Data: R.join('~', xs.map(getOriginalS3Key)), PartitionKey: kinesis_shards[i%kinesis_shards_count]})

    const params = {
        Records: R.splitEvery(config.kinesis.copy.keys, data).map(generateRecord),
        StreamName: kinesis_streamname
    }

    const msgCount = data.length
    const handleError = (err) => { console.log('Kinesis Error', err) }
    const handleResponse = (res) => {
        console.log(`Sent: ${msgCount} Failed: ${res.FailedRecordCount}`)
        // if something go wrong, will push all data again
        if (res.FailedRecordCount > 0) {
            timer.update(1)
            return sleep(timer.timesleep()).then(kinesis_producer(data))
        } else {
            timer.reset(7)
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
// timer for preventing flooding kinesis (result from FailedRecordCount)
const timer = counter()
timer.reset(7)

program.option('-p, --prefix <required>', 'S3 bucket in first level')
    .parse(process.argv)
const s3_prefix = program.prefix || null
// s3_prefix = 'document10001032/'
if (s3_prefix) {
    dumpRedis()
}
