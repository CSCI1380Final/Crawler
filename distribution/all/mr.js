const { getID } = require('../util/id.js');
const groupsModule = require('../local/groups');
const { id } = global.distribution.util;

function mr(config) {
  const context = {
    gid: config.gid || 'all',
  };

  function exec(configuration, cb) {
    console.log("[DEBUG] mr.exec() called with:", configuration);

    if (!configuration || !configuration.keys) {
      console.error("[ERROR] No config or no keys.");
      return cb(new Error("[MR] No config or no keys."), false);
    }

    const map_fn = configuration.map || null;
    const reduce_fn = configuration.reduce || null;

    // mr id used for tasks
    const mrId = 'mr-' + getID(Math.random());
    console.log("[ORCH] Generated mrId:", mrId);

    // orchestratorNotify 
    function orchestratorNotify(msg) {
      console.log("[ORCH] orchestratorNotify called with", msg);
      if (msg.phase === 'map_done') {
        console.log('[ORCH] Received map_done from worker, data:', msg.data);
        global.distribution.local[msg.mrId].onMapDone(msg.data);
      } else if (msg.phase === 'reduce_done') {
        console.log('[ORCH] Received reduce_done from worker, data:', msg.data);
        global.distribution.local[msg.mrId].onReduceDone(msg.data);
      }
    }

    // Local as the process for handling orchestrator notify
    global.distribution.local.routes.put(
      { notify: orchestratorNotify },
      mrId,
      (e, v) => {
        if (e) {
          console.error("[ERROR] Failed to register orchestrator route:", e);
          return cb(new Error(`[ORCH] Failed to register orchestrator: ${e.message}`), false);
        }
        console.log("[ORCH] Orchestrator route registered for", mrId);

        // map logistics
        const mapResults = [];
        let totalMapTasks = 0; 
        let completedMap = 0;

        // reduce logistics
        let totalReduceTasks = 0;
        let completedReduceCount = 0;
        const reduceAggregation = [];

        // For orchestratorNotify to access
        global.distribution.local[mrId] = {
          onMapDone: (data) => {
            console.log(`[ORCH] onMapDone called. current completedMap=${completedMap}, total=${totalMapTasks}`);
            mapResults.push(...data);
            completedMap++;
            console.log(`[ORCH] completedMap = ${completedMap}`);
            if (completedMap === totalMapTasks) {
              console.log("[ORCH] All map tasks done, now doReduce (with shuffle)...");
              doReduce();
            }
          },
          onReduceDone: (data) => {
            // When worker finished, send back the reduced data
            console.log(`[ORCH] onReduceDone called. data=`, data);
            reduceAggregation.push(...data);
            completedReduceCount++;
            console.log(`[ORCH] completedReduceCount = ${completedReduceCount}, totalReduceTasks=${totalReduceTasks}`);
            if (completedReduceCount === totalReduceTasks) {
              console.log("[ORCH] All reduce tasks done. Final results:", reduceAggregation);
              cb(null, reduceAggregation);
            }
          }
        };

        // assign map worker
        const nodeMap = groupsModule.get(context.gid);
        const nodeSet = new Set();
        for (const key of configuration.keys) {
          const nodeId = id.consistentHash(key, Object.keys(nodeMap));
          nodeSet.add(nodeId);
        }

        // worker service definition
        const mapWorker = {
          doMap: (msg, cb2) => {
            console.log("[WORKER] doMap() invoked with msg:", msg);
            if (!msg.map_fn) {
              return cb2(new Error('No map_fn'), false);
            }
            if (!msg.gid) {
              return cb2(new Error('No gid in msg!'), false);
            }
            const outputs = [];
            let doneCount = 0;

            for (const theKey of msg.keys) {
              global.distribution.local.store.get({ gid: msg.gid, key: theKey }, (err, value) => {
                if (err) return cb2(err);

                const partial = msg.map_fn(theKey, value);
                if (Array.isArray(partial)) {
                  outputs.push(...partial);
                } else {
                  outputs.push(partial);
                }
                doneCount++;
                if (doneCount === msg.keys.length) {
                  cb2(null, outputs);
                }
              });
            }
          }
        };

        // Register map worker to the assigned nodes
        let installed = 0;
        nodeSet.forEach(nodeId => {
          const node = nodeMap[nodeId];
          global.distribution.local.comm.send(
            [mapWorker, mrId + '-worker'],
            { node, service: 'routes', method: 'put' },
            (err) => {
              if (err) {
                console.error("[ERROR] Worker install failed on node", node, err);
                return cb(err);
              }
              installed++;
              if (installed === nodeSet.size) {
                // after install, start map process
                doMapPhase();
              }
            }
          );
        });

        function doMapPhase() {
          totalMapTasks = configuration.keys.length;
          for (const key of configuration.keys) {
            const nodeId = id.consistentHash(key, Object.keys(nodeMap));
            const node = nodeMap[nodeId];
            global.distribution.local.comm.send(
              [{ cmd: 'map', map_fn, keys: [key], gid: context.gid }],
              { node, service: mrId + '-worker', method: 'doMap' },
              (err, data) => {
                if (err) return cb(err);
                notifyOrchestrator({
                  phase: 'map_done',
                  mrId,
                  data,
                });
              }
            );
          }
        }

    
        function doReduce() {
          // group the map result
          const grouped = {};
          for (const pair of mapResults) {
            for (const k in pair) {
              if (!grouped[k]) grouped[k] = [];
              grouped[k].push(pair[k]);
            }
          }

          // resend to the node based on key
          const partitioned = {};
          const nodeIds = Object.keys(nodeMap);
          for (const k in grouped) {
            const nId = id.consistentHash(k, nodeIds);
            if (!partitioned[nId]) partitioned[nId] = {};
            partitioned[nId][k] = grouped[k];
          }
          const reduceNodeIds = Object.keys(partitioned);
          totalReduceTasks = reduceNodeIds.length;
          console.log("[ORCH] reduce partition info:", partitioned);

          // reduce worker logic
          const reduceWorker = {
            doReducePartition: (msg, cb2) => {
              const partialResult = [];
              for (const theKey in msg.partition) {
                const vals = msg.partition[theKey];
                const r = msg.reduce_fn(theKey, vals);
                // { key: result }
                partialResult.push(r);
              }
              cb2(null, partialResult);
            }
          };

          let installedReduceWorkers = 0;
          reduceNodeIds.forEach(nId => {
            const node = nodeMap[nId];
            global.distribution.local.comm.send(
              [reduceWorker, mrId + '-reduce-worker'],
              { node, service: 'routes', method: 'put' },
              (err) => {
                if (err) return cb(err);
                installedReduceWorkers++;
                if (installedReduceWorkers === reduceNodeIds.length) {
                  // after install reduce worker, start reduce work
                  startReduceTasks(partitioned);
                }
              }
            );
          });
        }

        function startReduceTasks(partitioned) {
          for (const nId in partitioned) {
            const node = nodeMap[nId];
            const p = partitioned[nId];
            global.distribution.local.comm.send(
              [{ partition: p, reduce_fn }],
              { node, service: mrId + '-reduce-worker', method: 'doReducePartition' },
              (err, partialReduceRes) => {
                if (err) return cb(err);
                // let orchestrator know that reduce is done
                notifyOrchestrator({
                  phase: 'reduce_done',
                  mrId,
                  data: partialReduceRes
                });
              }
            );
          }
        }

        // function send message to orchestrator 
        function notifyOrchestrator(msg) {
          global.distribution.local.comm.send(
            [msg],
            { node: global.nodeConfig, service: mrId, method: 'notify' },
            (err) => {
              if (err) console.error('[ORCH] notifyOrchestrator send error', err);
            }
          );
        }

      }
    );

  }

  return { exec };
}

module.exports = mr;
