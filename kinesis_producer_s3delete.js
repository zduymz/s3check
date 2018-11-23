const aws = require('aws-sdk')
const R = require('ramda')
const config = require('./config')
const program = require('commander')
const fs = require('fs');
const readline = require('readline');
const stream = require('stream');


/*
 *  Read from file and publish S3 keys need to be deleted to Kinesis, lambda_s3_delete.js will find all versions
 *  and delete them
 */

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
const kinesis_streamname = config.kinesis.delete.stream
const kinesis = new aws.Kinesis({region: config.kinesis.delete.region})
const kinesis_shards_count = config.kinesis.delete.shards
const kinesis_shards = R.range(1, config.kinesis.delete.shards + 1).map(generateShardId)

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

const startProcess = (filename) => {

    const instream = fs.createReadStream(filename);
    const outstream = new stream;
    const rl = readline.createInterface(instream, outstream);
    const limit = config.kinesis.delete.line_count
    const count = counter()
    let lines = []
    rl.on('line', function(line) {
        if(lines.length < limit) lines.push(line)
        else {
            count.update(lines.length)
            rl.pause()
            kinesis_producer(lines).then(() => {
                console.log('Resume reading file')
                // empty array before starting bumping it again
                lines.length = 0
                rl.resume()
            })

        }
    });

    rl.on('close', function() {
        if(lines.length > 0) {
            count.update(lines.length)
            kinesis_producer(lines).then(() => console.log(`Dumped ${count.total()} keys`))
        }
    });
}

const kinesis_producer = (data) => {
    // draft message, 5 keys into a record
    const generateRecord = (xs, i) => ({Data: R.join('~', xs.map(getOriginalS3Key)), PartitionKey: kinesis_shards[i%kinesis_shards_count]})

    const params = {
        Records: R.splitEvery(config.kinesis.delete.keys, data).map(generateRecord),
        StreamName: kinesis_streamname
    }

    const msgCount = data.length
    const handleError = (err) => { console.log('Kinesis Error', err) }
    const handleResponse = (res) => {
        console.log(`Sent: ${msgCount} Failed: ${res.FailedRecordCount}`)
        // if something go wrong, will push all data again
        if (res.FailedRecordCount > 0) {
            // backup a little bit if error occur
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

program.option('-f, --filename <required>', 'filename is similar to S3 bucket in first level')
    .parse(process.argv)
// check filename
const filename = program.filename || null
fs.stat(filename, (err, stat) => {
    if(err) {
        console.log(err)
        process.exit(1)
    }
    startProcess(filename)
})
