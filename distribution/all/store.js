// distribution/store.js (或 all/store.js)
const groupsModule = require('../local/groups');
const { id } = global.distribution.util;     // 用于 id.getID

function store(config) {
  const context = {};
  // 默认 gid='all', hash=naiveHash
  context.gid = config.gid || 'all';
  context.hash = config.hash || id.naiveHash;

  /**
   * 每次都从 groupsModule.get(gid) 获取节点信息 (无缓存简化)
   * @param {function(Error|null, object)} callback
   */
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
   * routeRequest - 哈希路由：若 key==null，用 state 生成 key；再 hash => node
   * @param {string} method - 'put' | 'get' | 'del'
   * @param {any} state - put时是要存储的对象，其它可为null
   * @param {string|null} key 
   * @param {function} callback
   */
  function routeRequest(method, state, key, callback) {
    getNodes((err, nodeIds, nodesMap) => {
      if (err) return callback(err);

      // 若 key==null，则将 state 序列化后做 sha256
      // (与本地 store 做法一致)
      let effectiveKey;
      if (key == null) {
        // 这里只做路由；真正写到本地也会再执行 “若 config=null => sha256(state)”，
        // 但为了哈希路由稳定，这里必须也先算出 key
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

      // 构造要发给本地 store 的参数
      let message;
      if (method === 'put') {
        // put => [state, keyString/null, callback]
        // 但我们要把 keyString 传过去，如果 null, 本地 store 才会自己算sha256
        // 这里为了满足 “no key => local store也会算hash”，我们可直接传 key(若 null)
        message = [state, key];
      } else {
        // get/del => [keyString, callback]
        // 同理，如果 key==null，本地 store 会算sha256(state)? 但 state=null
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
