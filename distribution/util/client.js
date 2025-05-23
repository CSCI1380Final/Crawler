const distribution = require('../../config.js');
const crypto = require('crypto');

function generateRandomPairs(n) {
  const pairs = [];
  for (let i = 0; i < n; i++) {
    const key = crypto.randomBytes(4).toString('hex');
    const obj = {
      value: Math.random(),
      index: i,
      timestamp: Date.now()
    };
    pairs.push({ key, obj });
  }
  return pairs;
}

const NUM_REQUESTS = 1000;
const pairs = generateRandomPairs(NUM_REQUESTS);

const awsNode = { ip: '3.144.48.151', port: 8001 };

function sendPutRequests(callback) {
  console.log("Stage 2: Sending PUT requests to AWS node...");
  const startTime = Date.now();
  let completed = 0;

  pairs.forEach(({ key, obj }) => {
    const message = [obj, key];
    const remoteSpec = {
      node: awsNode,
      service: 'store',
      method: 'put'
    };

    global.distribution.local.comm.send(message, remoteSpec, (err, res) => {
      if (err) {
        console.error(`PUT error for key ${key}:`, err);
      }
      completed++;
      if (completed === NUM_REQUESTS) {
        const totalTime = Date.now() - startTime;
        console.log(`PUT: ${NUM_REQUESTS} requests completed in ${totalTime} ms`);
        console.log(`PUT Throughput: ${(NUM_REQUESTS / (totalTime / 1000)).toFixed(2)} req/s`);
        console.log(`Average PUT Latency: ${(totalTime / NUM_REQUESTS).toFixed(2)} ms/request`);
        callback();
      }
    });
  });
}

function sendGetRequests(callback) {
  console.log("Stage 3: Sending GET requests to AWS node...");
  const startTime = Date.now();
  let completed = 0;

  pairs.forEach(({ key }) => {
    const message = [key];
    const remoteSpec = {
      node: awsNode,
      service: 'store',
      method: 'get'
    };

    global.distribution.local.comm.send(message, remoteSpec, (err, res) => {
      if (err) {
        console.error(`GET error for key ${key}:`, err);
      }
      completed++;
      if (completed === NUM_REQUESTS) {
        const totalTime = Date.now() - startTime;
        console.log(`GET: ${NUM_REQUESTS} requests completed in ${totalTime} ms`);
        console.log(`GET Throughput: ${(NUM_REQUESTS / (totalTime / 1000)).toFixed(2)} req/s`);
        console.log(`Average GET Latency: ${(totalTime / NUM_REQUESTS).toFixed(2)} ms/request`);
        callback();
      }
    });
  });
}

console.log("Stage 1: Generating random key-value pairs...");
console.log(`Generated ${NUM_REQUESTS} pairs.`);

sendPutRequests(() => {
  sendGetRequests(() => {
    console.log("Performance test completed.");
  });
});
