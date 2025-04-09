#!/usr/bin/env node

const distribution = require('../../config.js');
const fs = require('fs');
const path = require('path');
const id = distribution.util.id;

let clientResult = null; // 用于保存最终 MapReduce 结果

const awsNode = {
  ip: '3.144.48.151',
  port: 8001,
};

const awsNode2 = {
  ip : '52.14.114.169',
  port:1234
}

const localGid = { gid: 'research' };
const localGroup = {};
localGroup[id.getSID(awsNode)] = awsNode;
localGroup[id.getSID(awsNode2)] = awsNode2;

let localServer = null;

console.log('[LOCAL] Starting orchestrator with local.* ...');

distribution.node.start((server) => {
  localServer = server;
  console.log('[LOCAL] Orchestrator started on port', server.address().port);

  distribution.local.groups.put(localGid, localGroup, (err, v) => {
    console.log('[LOCAL] local.groups.put', err, v);
    if (err) {
      console.error('[LOCAL] local.groups.put error:', err);
      process.exit(1);
    }

    console.log('[LOCAL] AWS node registered in "local" group.');

    // 执行数据存储和 MapReduce
    const startTime = performance.now();
    storeTestData();
    const endTime = performance.now();
    console.log(`storeTestData() 执行时间: ${(endTime - startTime).toFixed(2)} 毫秒`);
  });
});

function storeTestData() {
  const urlsPath = path.join(__dirname, 'urls.txt');
  const rawUrls = fs.readFileSync(urlsPath, 'utf-8')
    .split('\n')
    .filter(line => line.trim() !== '');

  const dataset = rawUrls.map((url, i) => ({ [String(i)]: url }));
  const keys = dataset.map(entry => Object.keys(entry)[0]);

  let count = 0;

  dataset.forEach((entry) => {
    const key = Object.keys(entry)[0];
    const value = entry[key];
    distribution.research.store.put(value, key, (err) => {
      count++;
      if (err) {
        console.error(`[STORE] Error key=${key}`, err);
      }
      if (count === dataset.length) {
        console.log('[STORE] All URLs stored. Running MapReduce...');
        runMapReduce(keys);
      }
    });
  });
}


function runMapReduce(keys) {
  const mapFn = async (key, value) => {
    const url = value.trim();
    const page_id = url.split('/').pop();

    try {
      const res = await fetch(url);
      const html = await res.text();

      const titleMatch = html.match(/<title>(.*?)<\/title>/);
      const abstractMatch = html.match(/<blockquote class="abstract[^>]*>([\s\S]*?)<\/blockquote>/);

      const title = titleMatch ? titleMatch[1].replace(/\n/g, '').trim() : '';
      const abstract = abstractMatch ? abstractMatch[1].replace(/<[^>]*>/g, '').replace(/\n/g, '').trim() : '';

      const out = {};
      out[page_id] = { title, abstract, link: url, page_id };
      return [out];
    } catch (err) {
      console.error(`[MAP] Fetch failed for ${url}: ${err.message}`);
      return [];
    }
  };

  const reduceFn = (key, values) => {
    const out = {};
    out[key] = values[0]; // 保留第一个
    return out;
  };

  const mrStartTime = performance.now(); // <-- 添加这一行

  distribution.research.mr.exec({ keys, map: mapFn, reduce: reduceFn }, (err, results) => {
    const mrEndTime = performance.now(); // <-- 添加这一行

    if (err) {
      console.error('[MR] MapReduce error:', err);
      return;
    }

    console.log('[MR] Results:', results);
    const filePath = path.join(__dirname, 'result.json');

    try {
      fs.writeFileSync(filePath, JSON.stringify(results, null, 2), 'utf-8');
      console.log('✅ 结果已成功写入 result.json');
      const duration = (mrEndTime - mrStartTime).toFixed(2); // <-- 添加这一行
      console.log(`🕒 MapReduce 执行时间: ${duration} 毫秒`); // <-- 添加这一行
    } catch (err) {
      console.error('❌ 写入 JSON 文件出错:', err);
    }
  });
}



