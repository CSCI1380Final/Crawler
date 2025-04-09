#!/usr/bin/env node

/**
 * distributedCrawl.test.js
 * 
 */

const distribution = require('../config.js');
const id = distribution.util.id;

const ncdcGroup = {};
const avgwrdlGroup = {};
const cfreqGroup = {};

/*
    The local node will be the orchestrator.
*/
let localServer = null;

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};

const { execSync } = require('child_process');
const path = require('path');
const fs = require('fs');

/*
 *  (crawling) all.mr:crawl
 *  读 urls.txt, 把若干URL存到 ncdc store, 再用 map_fn 抓取 + getText + getURLs
 */
test('(crawling) all.mr:crawl', (done) => {
  // 1) 读取 urls.txt
  const urlsFilePath = path.join(__dirname, 'urls.txt');
  if (!fs.existsSync(urlsFilePath)) {
    return done(new Error("No urls.txt found in test folder"));
  }
  const lines = fs.readFileSync(urlsFilePath, 'utf-8')
    .split('\n')
    .map(l => l.trim())
    .filter(Boolean);

  // 2) 把 URL 存到 ncdc store, key="urlN"
  let putCount = 0;
  lines.forEach((url, i) => {
    const key = 'url' + i;
    distribution.ncdc.store.put(url, key, (err) => {
      putCount++;
      if (putCount === lines.length) {
        // 全部存完 -> 执行 MapReduce
        doCrawlMR(lines, done);
      }
    });
  });
});

// 定义 doCrawlMR
function doCrawlMR(lines, done) {
  const keys = lines.map((_, i) => 'url' + i);

  distribution.ncdc.mr.exec({
    keys,
    map: crawlMapFn,
    reduce: crawlReduceFn,
  }, (err, results) => {
    if (err) {
      return done(err);
    }
    console.log("(crawling) MR results:", results);
    // 这里可以根据需要做 expect(...) 测试
    done();
  });
}

// ============ map / reduce for crawling ============

// 同步地 调用 curl、getText.js、getURLs.js
function crawlMapFn(key, url) {
  let html;
  try {
    // 先 curl 下载
    html = execSync(`curl -s ${url}`, { encoding: 'utf-8' });
  } catch (e) {
    return [{ [`ERR_CURL:${url}`]: e.message }];
  }

  // 调用 getText.js
  let text;
  try {
    text = execSync(`echo "${shellEscape(html)}" | ${path.join(__dirname, 'getText.js')}`, {
      encoding: 'utf-8'
    });
  } catch (e) {
    return [{ [`ERR_GETTEXT:${url}`]: e.message }];
  }

  // 调用 getURLs.js
  let outlinksRaw;
  try {
    outlinksRaw = execSync(`echo "${shellEscape(html)}" | ${path.join(__dirname, 'getURLs.js')} ${url}`, {
      encoding: 'utf-8'
    });
  } catch (e) {
    return [{ [`ERR_GETURLS:${url}`]: e.message }];
  }

  const outlinks = outlinksRaw.split('\n').map(x => x.trim()).filter(Boolean);

  const results = [];
  // A) 返回文本
  const tObj = {};
  tObj[`TEXT:${url}`] = text;
  results.push(tObj);

  // B) 返回 outlinks
  for (const link of outlinks) {
    const lObj = {};
    lObj[`LINK:${link}`] = url;  // 表示 link来自于url
    results.push(lObj);
  }

  return results;
}

// 把 TEXT:xx 做拼接, 把 LINK:xx 去重
function crawlReduceFn(key, values) {
  const out = {};
  if (key.startsWith('TEXT:')) {
    out[key] = values.join('\n');
  } else if (key.startsWith('LINK:')) {
    out[key] = Array.from(new Set(values));
  } else if (key.startsWith('ERR_')) {
    out[key] = values;
  }
  return out;
}

function shellEscape(str) {
  return str.replace(/"/g, '\\"').replace(/\$/g, '\\$');
}

/*
    =============== beforeAll / afterAll 不改 ===============
*/
beforeAll((done) => {
  ncdcGroup[id.getSID(n1)] = n1;
  ncdcGroup[id.getSID(n2)] = n2;
  ncdcGroup[id.getSID(n3)] = n3;

  avgwrdlGroup[id.getSID(n1)] = n1;
  avgwrdlGroup[id.getSID(n2)] = n2;
  avgwrdlGroup[id.getSID(n3)] = n3;

  cfreqGroup[id.getSID(n1)] = n1;
  cfreqGroup[id.getSID(n2)] = n2;
  cfreqGroup[id.getSID(n3)] = n3;

  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, () => {
      distribution.local.status.spawn(n2, () => {
        distribution.local.status.spawn(n3, () => {
          cb();
        });
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    const ncdcConfig = {gid: 'ncdc'};
    startNodes(() => {
      distribution.local.groups.put(ncdcConfig, ncdcGroup, (e, v) => {
        distribution.ncdc.groups.put(ncdcConfig, ncdcGroup, (e, v) => {
          // 这里也保留对 avgwrdl / cfreq 的 put，不改
          const avgwrdlConfig = {gid: 'avgwrdl'};
          distribution.local.groups.put(avgwrdlConfig, avgwrdlGroup, (e, v) => {
            distribution.avgwrdl.groups.put(avgwrdlConfig, avgwrdlGroup, (e, v) => {
              const cfreqConfig = {gid: 'cfreq'};
              distribution.local.groups.put(cfreqConfig, cfreqGroup, (e, v) => {
                distribution.cfreq.groups.put(cfreqConfig, cfreqGroup, (e, v) => {
                  done();
                });
              });
            });
          });
        });
      });
    });
  });
});

afterAll((done) => {
  const remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, () => {
    remote.node = n2;
    distribution.local.comm.send([], remote, () => {
      remote.node = n3;
      distribution.local.comm.send([], remote, () => {
        localServer.close();
        done();
      });
    });
  });
});