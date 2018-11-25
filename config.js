const config = {
    s3: {
        source_bucket: 'bucket1',
        dest_bucket: 'bucket2'
    },

    kinesis: {
        copy: {
            stream: 'stream1',
            region: 'us-west-2',
            shards: 10,
            redis_count: 50000,
            keys: 200,
        },
        delete: {
            stream: 'stream2',
            region: 'us-west-2',
            shards: 10,
            line_count: 40000,
            keys: 100,
        }
    }
}

module.exports = config
