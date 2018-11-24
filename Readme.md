s3Check
===

#### This tool is created for my only usage purpose.  

---

I think this is interesting problem. And any one can see it in some cases when using AWS S3.  
So, a story is my company are using s3 to store some data. At first, they used bucket in us-west-1 for a while and relized should move that bucket to us-west-2 for cheaper price.  
My leader decided to use DistCP on EMR to copy whole files from bucket1 to bucket2. Everything worked well, except one thing. Wrong file name  
DistCP can not copy the file name like this
`bucket1/document123/0856/8e37/-801/2-41/f3-b/618-/3ce4/0078/1::png::200::490::bd`
Actually, it can copy but the name will not the same. The name will turn to
`bucket1/document123/document123/0856/8e37/-801/2-41/f3-b/618-/3ce4/0078/1`
Now you can see the problem. And because bucket2 Versioning is enabled. After running DistCp we have this  

| Bucket | number of files | total size | price a month |
| ------- |:---------------:|:----------:|:-------------:|
| bucket1 (us-west-1) | 1,052,476,781 | 419.85 TiB | $10,318.15 |
| bucket2 (us-west-2) | 1,783,464,681 | 696.26 TiB | $15,224.48 |

---

#### My Tasks
* Copy the files which were not copied correctly
* Clean the wrong files name

#### Ideas
You can see the s3 filename format `document123/0856/8e37/-801/2-41/f3-b/618-/3ce4/0078/xxx`  
We had around 300 documents (first sub level directory in bucket)  
I will write a small application read whole keys for each documents, store them in Redis. But how big is memory required?  
My key will be is `document123/08568e37-8012-41f3-b618-3ce40078xxx`, and value is empty.
```
127.0.0.1:6379> memory usage document123/08568e37-8012-41f3-b618-3ce400781
(integer) 91
```
So the size for each key around 100 bytes, for 100,000,000 keys, just need 10GB memory (that's not exactly true)
I guess for each documentxxx directory contain not over 100M files.  
```
node s3PrefixCheck.js -p document123 -m 0
```
`s3PrefixCheck.js` also run to check bucket2. It scan whole file in bucket2, and compare to keys in Redis.  
If key is found in Redis, it mean the file is copied correctly from bucket1 to bucket2, just delete that key in Redis.  
If key is not found in Redis, that file is not copied yet, it will write the key down to file name for later usage.  
```
node s3PrefixCheck.js -p document123 -m 1
```
Now, i got 2 places (redis and local file). The redis keys (file name) need to be copied from bucket1 to bucket2 and the keys in local file need to be removed in bucket2.  
I created 2 kinesis stream for publishing 2 types of key, and there will be 2 lambda function read 2 kinesis stream.  
This job will read redis and send messages (keys) to kinesis stream (call it stream1)
```
node kinesis_producer_s3copy.js -p document123/
```
This job will read local file and send message (keys) to kinesis stream (called it stream2)
```
node kinesis_producer_s3delete.js -f document123.txt
```
Both of them will send message in batch. For my limited knowledge about kinesis, look like i didn't utilize all its performance. Anyway i just want to experience it.  
I had 2 lambda function, got action based on kinesis records
```
lambda_s3_copy.js
lambda_s3_delete.js
```





