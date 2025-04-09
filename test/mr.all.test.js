const distribution = require('../config.js');
const id = distribution.util.id;

const ncdcGroup = {};
const avgwrdlGroup = {};
const cfreqGroup = {};

/*
    The local node will be the orchestrator.
*/
let localServer = null;

const n1 = {ip: '3.144.48.151', port: 8001};
const n2 = {ip: '3.144.48.151', port: 8001};
const n3 = {ip: '3.144.48.151', port: 8001};

const fs = require('fs');
const path = require('path');


test.only('(20 pts) all.mr:ncdc_arxiv_scraper', (done) => {
  // 1. 读取 urls.txt 文件
  const urlsPath = path.join(__dirname, 'urls.txt');
  const rawUrls = fs.readFileSync(urlsPath, 'utf-8').split('\n').filter(line => line.trim() !== '');

  // 2. 构造 dataset，和原格式保持一致：{ '0': url }
  const dataset = rawUrls.map((url, idx) => ({ [String(idx)]: url }));

  // 3. map 函数：爬取 arXiv 页面
  const mapper = async (key, value) => {
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
      console.error(`Failed to fetch ${url}: ${err.message}`);
      return [];
    }
  };

  // 4. reduce 函数：简单地去重，保留第一个即可
  const reducer = (key, values) => {
    const out = {};
    out[key] = values[0]; // 去重策略：保留第一个
    return out;
  };

  // 5. 运行 map reduce
  const doMapReduce = () => {
    distribution.ncdc.mr.exec(
      { keys: getDatasetKeys(dataset), map: mapper, reduce: reducer },
      (err, results) => {
        if (err) return done(err);
        try {
          const keys = results.map((obj) => Object.keys(obj)[0]);
          const uniqueKeys = new Set(keys);
          expect(keys.length).toBe(uniqueKeys.size); // 验证没有重复的 page_id
          done();
        } catch (e) {
          done(e);
        }
      }
    );
  };

  // 6. 先 put dataset，然后运行 MR
  let cntr = 0;
  dataset.forEach((entry) => {
    const key = Object.keys(entry)[0];
    const value = entry[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});


test('(20 pts) all.mr:avgwrdl', (done) => {
  // Calculate the average word length for each document
  const mapper = (key, value) => {
    const words = value.split(/\s+/).filter((e) => e !== '');
    const out = {};
    out[key] = {
      totalLength: words.reduce((sum, word) => sum + word.length, 0),
      wordCount: words.length,
    };
    return [out];
  };

  const reducer = (key, values) => {
    const totalLength = values.reduce((sum, v) => sum + v.totalLength, 0);
    const totalCount = values.reduce((sum, v) => sum + v.wordCount, 0);
    const avgLength = totalCount === 0 ? 0 : totalLength / totalCount;
    const out = {};
    out[key] = parseFloat(avgLength.toFixed(2));
    return out;
  };

  const dataset = [
    {'doca': 'short and simple sentence'},
    {'docb': 'another slightly longer example'},
    {'docc': 'the final example has various word lengths'},
  ];

  const expected = [
    {'doca': 5.5},
    {'docb': 7.0},
    {'docc': 5.14},
  ];

  const doMapReduce = (cb) => {
    distribution.avgwrdl.mr.exec({keys: getDatasetKeys(dataset), map: mapper, reduce: reducer}, (e, v) => {
      try {
        expect(v).toEqual(expect.arrayContaining(expected));
        done();
      } catch (e) {
        done(e);
      }
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.avgwrdl.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

test('(25 pts) all.mr:cfreq', (done) => {
  // Calculate the frequency of each character in a set of documents
  const mapper = (key, value) => {
    const chars = value.replace(/\s+/g, '').split('');
    const out = [];
    chars.forEach((char) => {
      const o = {};
      o[char] = 1;
      out.push(o);
    });
    return out;
  };

  const reducer = (key, values) => {
    const out = {};
    out[key] = values.reduce((sum, v) => sum + v, 0);
    return out;
  };

  const dataset = [
    {'doc1': 'hello world'},
    {'doc2': 'map reduce test'},
    {'doc3': 'character counting example'},
  ];

  const expected = [
    {'h': 2}, {'e': 7}, {'l': 4},
    {'o': 3}, {'w': 1}, {'r': 4},
    {'d': 2}, {'m': 2}, {'a': 4},
    {'p': 2}, {'u': 2}, {'c': 4},
    {'t': 4}, {'s': 1}, {'n': 2},
    {'i': 1}, {'g': 1}, {'x': 1},
  ];

  const doMapReduce = (cb) => {
    distribution.cfreq.mr.exec({keys: getDatasetKeys(dataset), map: mapper, reduce: reducer}, (e, v) => {
      try {
        expect(v).toEqual(expect.arrayContaining(expected));
        done();
      } catch (e) {
        done(e);
      }
    });
  };

  let cntr = 0;

  dataset.forEach((o) => {
    const key = Object.keys(o)[0];
    const value = o[key];
    distribution.cfreq.store.put(value, key, (e, v) => {
      cntr++;
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
});

/*
    Test setup and teardown
*/

// Helper function to extract keys from dataset (in case the get(null) funnctionality has not been implemented)
function getDatasetKeys(dataset) {
  return dataset.map((o) => Object.keys(o)[0]);
}

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
    // distribution.local.status.spawn(n1, (e, v) => {
      // distribution.local.status.spawn(n2, (e, v) => {
      //   distribution.local.status.spawn(n3, (e, v) => {
      //     cb();
      //   });
      // });
    // });
    cb()
  };

  distribution.node.start((server) => {
    localServer = server;

    const ncdcConfig = {gid: 'ncdc'};
    startNodes(() => {
      distribution.local.groups.put(ncdcConfig, ncdcGroup, (e, v) => {
        distribution.ncdc.groups.put(ncdcConfig, ncdcGroup, (e, v) => {
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
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        done();
      });
    });
  });
});
