const groupsModule = require('../local/groups');
const { id } = global.distribution.util;     

function store(config) {
  const context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || id.naiveHash;

  function routeRequest(method, state, key, callback) {
      console.log("igid", context.gid)
      console.log(context.hash)
      groupsModule.get(context.gid, (err, nodesMap) => {
        if (err || !nodesMap) {
          return callback(new Error(`Failed to get nodes for group "${context.gid}"`));
        }
        const nodeIds = Object.keys(nodesMap);
        if (nodeIds.length === 0) {
          return callback(new Error(`No nodes in group "${context.gid}"`));
        }
  
        if (method === 'get' && key == null) {
        let remaining = nodeIds.length;
        let keysAgg = [];
        let errors = {};
        nodeIds.forEach((nodeId) => {
          const nodeInfo = nodesMap[nodeId];
          console.log("node info is ", nodeInfo)
          global.distribution.local.comm.send(
            [{ key: null, gid: context.gid }],
            { node: nodeInfo, service: 'store', method: 'get' },
            (err, result) => {
              if (err) {
                console.log("error is ", err)
                errors[nodeId] = err;
              } else if (Array.isArray(result)) {
                console.log("the result", result)
                keysAgg = keysAgg.concat(result);
              }
              remaining--;
              if (remaining === 0) {
                return callback({}, keysAgg);
              }
            }
          );
        });
        return;
      }
  
        const effectiveKey = (key == null) ? id.getID(state) : key;
        const kid = id.getID(effectiveKey);
        console.log("kid is", kid)
  
        const targetNodeId = context.hash(kid, nodeIds);
        const targetNodeInfo = nodesMap[targetNodeId];
         console.log(targetNodeInfo)
        if (!targetNodeInfo) {
          return callback(new Error(`Node "${targetNodeId}" not found in group "${context.gid}"`));
        }
  
        let message;
        if (method === 'put') {
          message = [state, { key: effectiveKey, gid: context.gid }];
        } else {
          // get/del => [{ key, gid }]
          message = [{ key: effectiveKey, gid: context.gid }];
        }
  
        const remoteSpec = {
          node: targetNodeInfo,
          service: 'store',
          method: method
        };
  
        global.distribution.local.comm.send(message, remoteSpec, (error, value) => {
          callback(error, value);
        });
      });
    }

  return {
    get(configuration, callback) {
      routeRequest('get', null, configuration, callback);
    },
    put(state, configuration, callback) {
      routeRequest('put', state, configuration, callback);
    },
    del(configuration, callback) {
      routeRequest('del', null, configuration, callback);
    },
    reconf(configuration, callback) {
      callback = callback || function(e,v){
        if(e) console.error(e);
        else console.log(v);
      };
      if (configuration.gid) {
        context.gid = configuration.gid;
      }
      if (configuration.hash) {
        context.hash = configuration.hash;
      }
      if (configuration.nodes) {
        global.distribution.config = global.distribution.config || {};
        global.distribution.config.nodes = global.distribution.config.nodes || {};
        global.distribution.config.nodes[context.gid] = configuration.nodes;
      }
      callback(null, 'reconfigured');
    }
  };
}

module.exports = store;