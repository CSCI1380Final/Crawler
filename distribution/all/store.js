const groupsModule = require('../local/groups');
const { id } = global.distribution.util;     

function store(config) {
  const context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || id.naiveHash;

  function getNodes(callback) {
    groupsModule.get(context.gid, (err, nodesMap) => {
      if (err || !nodesMap) {
        return callback(new Error(`Failed to get nodes for group "${context.gid}"`));
      }
      const nodeIds = Object.keys(nodesMap);
      if (!nodeIds.length) {
        return callback(new Error(`No nodes in group "${context.gid}"`));
      }
      callback(null, nodeIds, nodesMap);
    });
  }

  /**
   * 
   * @param {string} method - 'put' | 'get' | 'del'
   * @param {any} state - store object for put
   * @param {string|null} key 
   * @param {function} callback
   */
  function routeRequest(method, state, key, callback) {
    getNodes((err, nodeIds, nodesMap) => {
      if (err) return callback(err);

      // if key==null，then state be sha256
      // (与本地 store 做法一致)
      let effectiveKey;
      if (key == null) {
        effectiveKey = id.getID(state);
      } else {
        effectiveKey = key;
      }

      const kid = id.getID(effectiveKey);
      const targetNodeId = context.hash(kid, nodeIds);
      const targetNodeInfo = nodesMap[targetNodeId];
      if (!targetNodeInfo) {
        return callback(new Error(`Node "${targetNodeId}" not found in group "${context.gid}"`));
      }

      let message;
      if (method === 'put') {
        message = [state, key];
      } else {
        message = [effectiveKey];
      }

      const remoteSpec = {
        node: targetNodeInfo,
        service: 'store',
        method
      };

      global.distribution.local.comm.send(message, remoteSpec, callback);
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
