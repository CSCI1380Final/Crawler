const { getID } = require('../util/id.js');
const groupsModule = require('../local/groups');

/**
 * 创建一个 MapReduce 对象
 * @param {Object} config
 * @returns {{ exec: Function }}
 */
function mr(config) {
  const context = {
    gid: config.gid || 'all',
  };

  /**
   * 核心入口
   * @param {Object} configuration
   * @param {Function} cb
   */
  function exec(configuration, cb) {
    console.log("[DEBUG] mr.exec() called with:", configuration);

    if (!configuration || !configuration.keys) {
      console.error("[ERROR] No config or no keys.");
      return cb(new Error("[MR] No config or no keys."), false);
    }

    const map_fn = configuration.map || null;
    const reduce_fn = configuration.reduce || null;

    // 给这次MR生成唯一ID
    const mrId = 'mr-' + getID(Math.random());
    console.log("[ORCH] Generated mrId:", mrId);

    /**
     * orchestratorNotify: orchestrator在本地的路由回调
     */
    function orchestratorNotify(msg) {
      console.log("[ORCH] orchestratorNotify called with", msg);
      if (msg.phase === 'map_done') {
        global.distribution.local[msg.mrId].onMapDone(msg.nodeId);
      } else if (msg.phase === 'shuffle_done') {
        global.distribution.local[msg.mrId].onShuffleDone(msg.nodeId);
      } else if (msg.phase === 'reduce_done') {
        global.distribution.local[msg.mrId].onReduceDone(msg.data);
      }
    }

    // 注册一个 orchestrator 路由
    global.distribution.local.routes.put({ notify: orchestratorNotify }, mrId, (e, v) => {
      if (e) {
        console.error("[ERROR] Failed to register orchestrator route:", e);
        return cb(new Error(`[ORCH] Failed to register orchestrator: ${e.message}`), false);
      }
      console.log("[ORCH] Orchestrator route registered for", mrId);

      // 以下是分发阶段的一些变量
      let completedMapNodes = new Set();
      let completedShuffleNodes = new Set();
      let completedReduceCount = 0;
      let totalReduceTasks = 0;
      const reduceAggregation = [];

      // 在 orchestrator 上，用来接收 worker 的通知
      global.distribution.local[mrId] = {
        onMapDone: (nodeId) => {
          completedMapNodes.add(nodeId);
          if (completedMapNodes.size === nodeSet.size) {
            console.log("[ORCH] All map tasks done, now starting shuffle phase...");
            startShufflePhase();
          }
        },
        onShuffleDone: (nodeId) => {
          completedShuffleNodes.add(nodeId);
          if (completedShuffleNodes.size === nodeSet.size) {
            console.log("[ORCH] All shuffle tasks done, now starting reduce phase...");
            startReducePhase();
          }
        },
        onReduceDone: (data) => {
          reduceAggregation.push(...data);
          completedReduceCount++;
          if (completedReduceCount === totalReduceTasks) {
            console.log("[ORCH] All reduce tasks done. Final results:", reduceAggregation);
            cb(null, reduceAggregation);
          }
        }
      };

      // 获取group内节点信息
      const nodeMap = groupsModule.get(context.gid);
      const nodeIds = Object.keys(nodeMap);
      // 计算要把keys分配给哪些节点
      const nodeSet = new Set();
      for (const key of configuration.keys) {
        const nodeId = globalModuleHash(key, nodeIds);
        nodeSet.add(nodeId);
      }

      /**
       * 定义要在 worker 上执行的方法
       * 关键：我们要把“需要序列化”的函数都放进这个对象里
       */
      const mapWorker = {
        /**
         * doMap: 支持 async map_fn
         */
        doMap: (msg, cb2) => {
          (async () => {
            console.log("[WORKER] doMap() invoked:", msg);
            const { map_fn, gid, keys, nodeId, mrId, require } = msg;
            if (!map_fn) throw new Error('No map_fn provided');
            if (!gid) throw new Error('No gid in msg');

            if (!global.mapResults) {
              global.mapResults = [];
            }

            // 依次处理 keys
            for (const theKey of keys) {
              // 1) 从 store 获取
              const value = await new Promise((resolve, reject) => {
                global.distribution.local.store.get({ gid, key: theKey }, (err, val) => {
                  if (err) return reject(err);
                  resolve(val);
                });
              });
              // 2) 调用 map_fn (async)
              const partial = await map_fn(theKey, value, require);

              // 3) push 到全局
              if (Array.isArray(partial)) {
                global.mapResults.push(...partial);
              } else {
                global.mapResults.push(partial);
              }
            }

            // 通知 orchestrator，map_done
            return { done: true, nodeId };
          })()
            .then(result => cb2(null, result))
            .catch(err => cb2(err));
        },

        /**
         * doShuffle: 需要把 nodeAddrs、nodeIds、mrId 从 msg 解构出来
         * 并内嵌 localModuleHash 函数来做哈希分配
         */
        doShuffle: (msg, cb2) => {
          console.log("[WORKER] doShuffle() invoked");
          const { nodeAddrs, nodeIds, nodeId, mrId } = msg;

          // 内嵌的哈希函数
          function localModuleHash(key, arr) {
            arr.sort();
            return arr[localIdToNum(key) % arr.length];
          }
          function localIdToNum(id) {
            let hash = 0;
            for (let i = 0; i < id.length; i++) {
              hash = ((hash << 5) - hash) + id.charCodeAt(i);
              hash = hash & hash;
            }
            return Math.abs(hash);
          }

          if (!global.mapResults || global.mapResults.length === 0) {
            return cb2(null, { done: true, nodeId });
          }

          // 1) 分组
          const grouped = {};
          for (const pair of global.mapResults) {
            for (const k in pair) {
              if (!grouped[k]) grouped[k] = [];
              grouped[k].push(pair[k]);
            }
          }

          // 2) 分区
          const partitioned = {};
          for (const k in grouped) {
            const nId = localModuleHash(k, nodeIds);
            if (!partitioned[nId]) partitioned[nId] = {};
            partitioned[nId][k] = grouped[k];
          }

          console.log("[WORKER] Shuffle completed. Partitions:", Object.keys(partitioned).length);

          // 自己要处理的分区
          global.shufflePartition = partitioned[nodeId] || {};

          // 给其他节点发送分区数据
          const sendTargets = Object.keys(partitioned).filter(n => n !== nodeId);
          if (sendTargets.length === 0) {
            return cb2(null, { done: true, nodeId });
          }

          let sendCount = 0;
          for (const targetNodeId of sendTargets) {
            const targetNode = nodeAddrs[targetNodeId];  // IP/port
            global.distribution.local.comm.send(
              [{
                cmd: 'receiveShuffleData',
                partition: partitioned[targetNodeId],
                sourceNodeId: nodeId
              }],
              { node: targetNode, service: mrId + '-worker', method: 'receiveShuffleData' },
              (err) => {
                if (err) {
                  console.error("[WORKER] Failed to send shuffle data to", targetNodeId, err);
                  return cb2(err);
                }
                sendCount++;
                if (sendCount === sendTargets.length) {
                  cb2(null, { done: true, nodeId });
                }
              }
            );
          }
        },

        /**
         * receiveShuffleData: 合并别的节点发来的分区
         */
        receiveShuffleData: (msg, cb2) => {
          if (!global.shufflePartition) {
            global.shufflePartition = {};
          }
          const { partition, sourceNodeId } = msg;
          for (const k in partition) {
            if (!global.shufflePartition[k]) {
              global.shufflePartition[k] = [];
            }
            global.shufflePartition[k].push(...partition[k]);
          }
          cb2(null, { received: true, sourceNodeId });
        },

        /**
         * doReduce
         */
        doReduce: (msg, cb2) => {
          console.log("[WORKER] doReduce() invoked");
          const { reduce_fn, nodeId, mrId } = msg;

          if (!global.shufflePartition) {
            return cb2(null, []);
          }

          const partialResult = [];
          for (const theKey in global.shufflePartition) {
            const vals = global.shufflePartition[theKey];
            const r = reduce_fn(theKey, vals);
            partialResult.push(r);
          }
          // 清理
          delete global.mapResults;
          delete global.shufflePartition;

          cb2(null, partialResult);
        }
      };

      // 在所有要参与的节点上安装 mapWorker
      const nodeSetArr = Array.from(nodeSet);
      let installedCount = 0;
      nodeSetArr.forEach(nId => {
        const node = nodeMap[nId];
        global.distribution.local.comm.send(
          [mapWorker, mrId + '-worker'], 
          { node, service: 'routes', method: 'put' },
          (err) => {
            if (err) {
              console.error("[ERROR] Worker install failed on node", node, err);
              return cb(err);
            }
            installedCount++;
            if (installedCount === nodeSetArr.length) {
              // 全部 worker service 安装完毕 -> map阶段
              doMapPhase();
            }
          }
        );
      });

      /**
       * doMapPhase
       */
      function doMapPhase() {
        const nodeKeysMap = {};
        for (const key of configuration.keys) {
          const nId = globalModuleHash(key, nodeIds);
          if (!nodeKeysMap[nId]) nodeKeysMap[nId] = [];
          nodeKeysMap[nId].push(key);
        }

        // 给每个节点发送map任务
        for (const nId in nodeKeysMap) {
          const node = nodeMap[nId];
          global.distribution.local.comm.send(
            [{
              cmd: 'map',
              map_fn,
              keys: nodeKeysMap[nId],
              gid: context.gid,
              nodeId: nId,
              mrId,
              require: configuration.require
            }],
            { node, service: mrId + '-worker', method: 'doMap' },
            (err, data) => {
              if (err) return cb(err);
              notifyOrchestrator({
                phase: 'map_done',
                mrId,
                nodeId: data.nodeId
              });
            }
          );
        }
      }

      /**
       * startShufflePhase
       */
      function startShufflePhase() {
        console.log("[ORCH] Starting shuffle phase...");
        const nodeIdsArr = Array.from(nodeSet);
        // 准备 nodeAddrs
        const nodeAddrs = {};
        nodeIdsArr.forEach(nId => {
          nodeAddrs[nId] = nodeMap[nId]; // { ip, port, ... }
        });
        
        nodeIdsArr.forEach(nId => {
          const node = nodeMap[nId];
          global.distribution.local.comm.send(
            [{
              cmd: 'shuffle',
              nodeId: nId,
              nodeIds: nodeIdsArr,
              nodeAddrs,
              mrId
            }],
            { node, service: mrId + '-worker', method: 'doShuffle' },
            (err, data) => {
              if (err) {
                console.error("[ERROR] Shuffle failed on node", nId, err);
                return cb(err);
              }
              notifyOrchestrator({
                phase: 'shuffle_done',
                mrId,
                nodeId: data.nodeId
              });
            }
          );
        });
      }

      /**
       * startReducePhase
       */
      function startReducePhase() {
        totalReduceTasks = nodeSet.size;
        nodeSet.forEach(nId => {
          const node = nodeMap[nId];
          global.distribution.local.comm.send(
            [{
              reduce_fn,
              mrId,
              nodeId: nId
            }],
            { node, service: mrId + '-worker', method: 'doReduce' },
            (err, partialReduceRes) => {
              if (err) {
                console.error("[ERROR] Reduce failed on node", nId, err);
                return cb(err);
              }
              notifyOrchestrator({
                phase: 'reduce_done',
                mrId,
                data: partialReduceRes
              });
            }
          );
        });
      }

      /**
       * orchestrator -> orchestrator route
       */
      function notifyOrchestrator(msg) {
        global.distribution.local.comm.send(
          [msg],
          { node: global.nodeConfig, service: mrId, method: 'notify' },
          (err) => {
            if (err) console.error('[ORCH] notifyOrchestrator send error', err);
          }
        );
      }
    });
  }

  return { exec };
}

function globalModuleHash(key, nodeIds) {
  nodeIds.sort();
  return nodeIds[idToNum(key) % nodeIds.length];
}
function idToNum(id) {
  let hash = 0;
  for (let i = 0; i < id.length; i++) {
    hash = ((hash << 5) - hash) + id.charCodeAt(i);
    hash = hash & hash;
  }
  return Math.abs(hash);
}

module.exports = mr;
