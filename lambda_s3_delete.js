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
        handlerDelete(keys);
    });
};

const generateObjects = (data) => R.chain((x) => concatAll(x), data).map(R.zipObj(['Key', 'VersionId']));

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







const payload = 'buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/15~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/16~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/17~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/18~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/19~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/2~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/20~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/21~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/3~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/4~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/5~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/6~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/7~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/8~buck10000332/bl61/0ab8/15-e/b68-/4c89/-862/7-98/d8fe/9~buck10000332/bl61/0e37/38-2/4c2-/4a65/-8bd/1-b1/52f2/-1~buck10000332/bl61/0e37/38-2/4c2-/4a65/-8bd/1-b1/52f2/0~buck10000332/bl61/0e37/38-2/4c2-/4a65/-8bd/1-b1/52f2/1~buck10000332/bl61/0e37/38-2/4c2-/4a65/-8bd/1-b1/52f2/2~buck10000332/bl61/0e37/38-2/4c2-/4a65/-8bd/1-b1/52f2/3~buck10000332/bl61/0e37/38-2/4c2-/4a65/-8bd/1-b1/52f2/4~buck10000332/bl61/0e37/38-2/4c2-/4a65/-8bd/1-b1/52f2/5~buck10000332/bl61/0e37/38-2/4c2-/4a65/-8bd/1-b1/52f2/6~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/-1~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/0~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/1~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/10~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/11~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/12~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/13~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/14~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/15~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/16~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/17~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/18~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/19~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/2~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/20~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/21~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/22~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/23~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/24~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/25~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/26~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/27~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/28~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/29~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/3~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/30~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/31~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/32~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/33~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/34~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/35~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/36~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/37~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/38~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/39~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/4~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/40~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/41~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/42~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/43~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/44~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/45~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/46~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/47~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/48~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/49~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/5~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/50~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/51~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/52~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/53~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/54~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/55~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/56~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/57~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/58~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/59~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/6~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/60~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/61~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/62~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/63~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/64~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/65~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/66~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/67~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/68~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/69~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/7~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/70~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/71~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/72~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/73~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/74~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/75~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/76~buck10000332/bl61/1351/10-5/797-/4d4a/-b28/f-b7/af48/77'
        Promise.all(payload.split('~').map(s3GetObjectVersions))
            .then(generateObjects)
            .then(s3Delete)
            .then(respHandler);

