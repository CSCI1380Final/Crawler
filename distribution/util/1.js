#!/usr/bin/env node

const distribution = require('../../config.js');
const fs = require('fs');
const path = require('path');
const id = distribution.util.id;

let clientResult = null; // 用于保存最终 MapReduce 结果
//
const awsNode = {
  ip: '18.118.154.30',
  port: 8001,
};

const awsNode2 = {
  ip : '52.14.114.169',
  port:1234
}
//
const awsNode3 = {
  ip : '3.140.243.195',
  port: 8001
}

const awsNode4 = {
  ip: '18.117.99.129',
  port: 8001
}
//
const awsNode5 = {
  ip: '3.144.117.196',
  port: 8001
}
//
const awsNode6 = {
  ip: '18.119.107.66',
  port: 8001
}
//
const awsNode7 = {
  ip: '3.142.131.248',
  port: 8001
}

const awsNode8 = {
  ip : '3.17.79.4',
  port : 8001
}

const awsNode9 = {
  ip : '18.117.99.129',
  port : 8001
}

const awsNode10 = {
  ip : '18.222.162.66',
  port : 8001
}

const awsNode11 = {
  ip : '54.90.65.103',
  port : 8001
}

const awsNode12 = {
  ip : '50.17.35.28',
  port : 8001
}

const awsNode13 = {
  ip : '54.91.105.174',
  port : 8001
}

const awsNode14 = {
  ip : '18.117.158.241',
  port : 8001
}

const awsNode15 = {
  ip : '18.224.58.12',
  port: 8001
}


// const aiyunNode = {
//   ip : '3.145.100.105',
//   port:9003
// }


const localNode = {
  ip : 'localhost',
  port:8001
}

const localGid = { gid: 'research' };
const localGroup = {};
localGroup[id.getSID(awsNode)] = awsNode;
// // localGroup[id.getSID(awsNode2)] = awsNode2;
// localGroup[id.getSID(awsNode3)] = awsNode3;
// localGroup[id.getSID(awsNode4)] = awsNode4;
// localGroup[id.getSID(awsNode5)] = awsNode5;
// localGroup[id.getSID(awsNode6)] = awsNode6;
// localGroup[id.getSID(awsNode7)] = awsNode7;
// localGroup[id.getSID(awsNode8)] = awsNode8;
// localGroup[id.getSID(awsNode9)] = awsNode9;
// localGroup[id.getSID(awsNode10)] = awsNode10;
// localGroup[id.getSID(aiyunNode)] = aiyunNode;
// // localGroup[id.getSID(localNode)] = localNode;
// localGroup[id.getSID(awsNode11)] = awsNode11;
// localGroup[id.getSID(awsNode12)] = awsNode12
// localGroup[id.getSID(awsNode13)] = awsNode13
// localGroup[id.getSID(awsNode14)] = awsNode14
// localGroup[id.getSID(awsNode15)] = awsNode15



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
    const paper_id = url.split('/').pop();

    try {
      const res = await fetch(url);
      const html = await res.text();

      let titleMatch = html.match(/<h1 class="title mathjax">.*?<\/span>(.*?)<\/h1>/);
      const abstractMatch = html.match(/<blockquote class="abstract[^>]*>([\s\S]*?)<\/blockquote>/);

      const title = titleMatch ? titleMatch[1].replace(/\n/g, '').trim() : '';
      const abstract = abstractMatch ? abstractMatch[1].replace(/<[^>]*>/g, '').replace(/\n/g, '').trim() : '';

      const out = {};
      out[paper_id] = { title, abstract, link: url, paper_id };
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

    const transformed = results.map(obj => {
    const key = Object.keys(obj)[0];
    const { title, abstract, link, paper_id } = obj[key];
    return {
      paper_id: paper_id,  
      title,
      abstract,
      link
    };
  });
    
    const filePath = path.join(__dirname, 'result.json');

    try {
      fs.writeFileSync(filePath, JSON.stringify(results, null, 2), 'utf-8');
      console.log('✅ 结果已成功写入 result.json');
      const duration = (mrEndTime - mrStartTime).toFixed(2); // <-- 添加这一行
      console.log(transformed)

    //   fetch('http://39.101.70.173:10086/batchInsert', {
    //   method: 'POST',
    //   headers: {
    //     'Content-Type': 'application/json'
    //   },
    //   body: JSON.stringify(transformed)
    // })
    //   .then(res => res.text())
    //   .then(data => {
    //     console.log("✅ 成功响应：", data);
    //   })
    //   .catch(err => {
    //     console.error("❌ 请求失败：", err);
    //   });

      console.log(`🕒 MapReduce 执行时间: ${duration} 毫秒`); // <-- 添加这一行

    } catch (err) {
      console.error('❌ 写入 JSON 文件出错:', err);
    }
  });
}



