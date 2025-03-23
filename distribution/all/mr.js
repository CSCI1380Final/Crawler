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

        const mapResults = [];
        let totalMapTasks = 0; 
        let completedMap = 0;

        global.distribution.local[mrId] = {
          onMapDone: (data) => {
            console.log(`[ORCH] onMapDone called. current completedMap=${completedMap}, total=${totalMapTasks}`);
            console.log("[ORCH] onMapDone data:", data);
            mapResults.push(...data);
            completedMap++;
            console.log(`[ORCH] completedMap = ${completedMap}`);
            if (completedMap === totalMapTasks) {
              console.log("[ORCH] All map tasks done, now doReduce...");
              doReduce();
            }
          }
        };

        const nodeMap = groupsModule.get(context.gid);
        console.log("[ORCH] nodeMap is", nodeMap);
        const nodeSet = new Set();
        for (const key of configuration.keys) {
          const nodeId = id.consistentHash(key, Object.keys(nodeMap));
          nodeSet.add(nodeId);
        }
        console.log("[ORCH] Unique nodeSet:", nodeSet);

        // worker service definition
        const mapWorker = {
          doMap: (msg, cb2) => {
            console.log("[WORKER] doMap() invoked with msg:", msg);
            if (!msg.map_fn) {
              console.error("[WORKER] No map_fn in msg.");
              return cb2(new Error('No map_fn'), false);
            }
            if (!msg.gid) {
              console.error("[WORKER] No gid in msg.");
              return cb2(new Error('No gid in msg!'), false);
            }
            const outputs = [];
            let doneCount = 0;

            for (const theKey of msg.keys) {
              console.log("[WORKER] store.get key=", theKey);
              global.distribution.local.store.get({ gid: msg.gid, key: theKey }, (err, value) => {
                if (err) {
                  console.error("[WORKER] store.get error:", err);
                  return cb2(err);
                }
                console.log(`[WORKER] map_fn(${theKey}, ${value})`);
                const partial = msg.map_fn(theKey, value);
                if (Array.isArray(partial)) {
                  outputs.push(...partial);
                } else {
                  outputs.push(partial);
                }
                doneCount++;
                if (doneCount === msg.keys.length) {
                  console.log("[WORKER] doMap finished, outputs=", outputs);
                  cb2(null, outputs);
                }
              });
            }
          }
        };

        // Register mapWorker service to worker 
        let installed = 0;
        nodeSet.forEach(nodeId => {
          const node = nodeMap[nodeId];
          console.log(`[ORCH] Installing worker -> ${mrId}-worker on node:`, node);
          global.distribution.local.comm.send(
            [mapWorker, mrId + '-worker'],
            { node, service: 'routes', method: 'put' },
            (e, v) => {
              if (e) {
                console.error("[ERROR] Worker install failed on node", node, e);
                return cb(e);
              }
              console.log(`[ORCH] Worker installed on node:`, node);
              installed++;
              console.log(`[ORCH] installed=${installed}, nodeSet.size=${nodeSet.size}`);
              if (installed === nodeSet.size) {
                console.log("[ORCH] All workers installed, start doMapPhase...");
                doMapPhase();
              }
            }
          );
        });

        function doMapPhase() {
          totalMapTasks = configuration.keys.length;
          console.log("[ORCH] doMapPhase -> totalMapTasks=", totalMapTasks);

          for (const key of configuration.keys) {
            const nodeId = id.consistentHash(key, Object.keys(nodeMap));
            const node = nodeMap[nodeId];
            console.log(`[ORCH] Sending map request for key='${key}' to nodeId='${nodeId}'`, node);
            global.distribution.local.comm.send(
              [{ cmd: 'map', map_fn, keys: [key], gid: context.gid }],
              { node, service: mrId + '-worker', method: 'doMap' },
              (err, data) => {
                if (err) {
                  console.error("[ERROR] map task error for key=", key, err);
                  return cb(err);
                }
                console.log(`[ORCH] map done for key='${key}' -> data=`, data);

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
          console.log("[ORCH] doReduce with mapResults:", mapResults);

          const grouped = {};
          for (const pair of mapResults) {
            for (const k in pair) {
              if (!grouped[k]) grouped[k] = [];
              grouped[k].push(pair[k]);
            }
          }

          const reduced = [];
          for (const k in grouped) {
            const r = reduce_fn(k, grouped[k]);
            reduced.push(r);
          }
          console.log("[ORCH] doReduce -> final result:", reduced);
          cb(null, reduced);
        }

        function notifyOrchestrator(msg) {
          console.log("[ORCH] notifyOrchestrator ->", msg);
          // RPC:
          global.distribution.local.comm.send(
            [msg],
            { node: global.nodeConfig, service: mrId, method: 'notify' },
            (e, v) => {
              if (e) {
                console.error('[ORCH] notifyOrchestrator send error', e);
              } else {
                console.log('[ORCH] notifyOrchestrator done');
              }
            }
          );
        }

      }
    );

    // orchestrator notify
    function orchestratorNotify(msg) {
      console.log("[ORCH] orchestratorNotify called with", msg);
      if (msg.phase === 'map_done') {
        console.log('[ORCH] Received map_done from worker, data:', msg.data);
        global.distribution.local[msg.mrId].onMapDone(msg.data);
      }
    }

  }

  return { exec };
}

module.exports = mr;
