#!/usr/bin/env node

/**
 * awsClientLocal.js
 *
 * 本地脚本，用来连接 AWS 上的节点（ip=3.144.48.151, port=8001）
 * 并发起一个简单的 MapReduce 任务，使用 local.* 命名空间。
 */

const distribution = require('../../config.js');  // 你的实际项目路径
const id = distribution.util.id;

// ========== 1) 定义远程节点 ==========
const awsNode = {
  ip: '3.144.48.151',
  port: 8001,
};

// 我们使用 "local" 这个gid
const localGid = { gid: 'local' };
const localGroup = {};

// 把远程节点映射到 localGroup
localGroup[id.getSID(awsNode)] = awsNode;

// ========== 2) 启动本地 orchestrator ==========
let localServer = null;
console.log('[LOCAL] Starting orchestrator with local.* ...');

distribution.node.start((server) => {
  localServer = server;
  console.log('[LOCAL] Orchestrator started on port', server.address().port);

  // 向 local.groups.put 注册远程节点
  distribution.local.groups.put(localGid, localGroup, (err) => {
    if (err) {
      console.error('[LOCAL] local.groups.put error:', err);
      process.exit(1);
    } 
    console.log('[LOCAL] AWS node registered in "local" group.');

    // 3) 放一些数据
    storeTestData();
  });
});

// ========== 3) 往 local.store 放一些测试数据 ==========
function storeTestData() {
  const dataset = {
    alpha: 'Hello from local orchestrator!',
    beta: 'Another line of data, second.',
  };

  const keys = Object.keys(dataset);
  let count = 0;

  keys.forEach((k) => {
    distribution.local.store.put(dataset[k], k, (err) => {
      count++;
      if (err) {
        console.error('Error storing key=', k, 'err=', err);
      }
      if (count === keys.length) {
        console.log('[LOCAL] All test data stored. Now run mapreduce.');
        runMapReduce(keys);
      }
    });
  });
}

// ========== 4) 运行简单的 map/reduce (local.mr.exec) ==========
function runMapReduce(keys) {
  const mapFn = (key, value) => {
    // 分词后，每个单词产出 { word: 1 }
    const words = value.split(/\s+/).filter(Boolean);
    return words.map((w) => ({ [w]: 1 }));
  };

  const reduceFn = (key, values) => {
    // 累加
    const total = values.reduce((acc, n) => acc + n, 0);
    return { [key]: total };
  };

  distribution.local.mr.exec({ keys, map: mapFn, reduce: reduceFn }, (err, results) => {
    if (err) {
      console.error('[LOCAL] MapReduce error:', err);
      return;
    }
    console.log('[LOCAL] MapReduce Results:', results);

    // 你可以在这里 close localServer，也可以让脚本继续跑
    // localServer.close(() => console.log('Done.'));
  });
}
