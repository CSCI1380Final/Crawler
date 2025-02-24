const groupsModule = require('../local/groups');
const { id } = global.distribution.util;

/**
 * 分布式 mem 服务（简化版，无 init 缓存）
 * 当 key == null 时，用 state 的 sha256 做哈希路由。
 */
function mem(config) {
  const context = {};
  // 若未指定 gid 用 'all'
  context.gid = config.gid || 'all';
  // 若未指定 hash 用 naiveHash
  context.hash = config.hash || id.naiveHash;

  /**
   * 计算路由并发送请求
   * @param {string} method - "get", "put", or "del"
   * @param {any} state - put 时是存储的对象，其它方法可为 null
   * @param {string|null} key - 主键；若 null, 用 state 生成
   * @param {function} callback
   */
  function routeRequest(method, state, key, callback) {
    // 每次都获取组信息（不做缓存）
    groupsModule.get(context.gid, (err, nodesMap) => {
      if (err || !nodesMap) {
        return callback(new Error(`Failed to get nodes for group "${context.gid}"`));
      }
      const nodeIds = Object.keys(nodesMap);
      if (nodeIds.length === 0) {
        return callback(new Error(`No nodes in group "${context.gid}"`));
      }

      // 若 key == null，用 state 做 sha256
      const effectiveKey = (key == null) ? id.getID(state) : key;
      const kid = id.getID(effectiveKey);

      // 通过 hash 决定要发送到哪个节点
      const targetNodeId = context.hash(kid, nodeIds);
      const targetNodeInfo = nodesMap[targetNodeId];
      if (!targetNodeInfo) {
        return callback(new Error(`Node "${targetNodeId}" not found in group "${context.gid}"`));
      }

      // 组装要发给本地 mem 的参数
      let message;
      if (method === 'put') {
        // put => [state, { key, gid }]
        message = [state, { key: effectiveKey, gid: context.gid }];
      } else {
        // get/del => [{ key, gid }]
        message = [{ key: effectiveKey, gid: context.gid }];
      }

      const remoteSpec = {
        node: targetNodeInfo,
        service: 'mem',
        method: method
      };

      global.distribution.local.comm.send(message, remoteSpec, (error, value) => {
        callback(error, value);
      });
    });
  }

  return {
    /**
     * get
     * @param {string|null} configuration - 主键（可为 null）
     * @param {function} callback
     */
    get: (configuration, callback) => {
      routeRequest('get', null, configuration, callback);
    },

    /**
     * put
     * @param {any} state - 要存储的对象
     * @param {string|null} configuration - 主键（若 null 则基于 state 生成）
     * @param {function} callback
     */
    put: (state, configuration, callback) => {
      routeRequest('put', state, configuration, callback);
    },

    /**
     * del
     * @param {string|null} configuration - 主键（若 null 则基于 null => ''?）
     * @param {function} callback
     */
    del: (configuration, callback) => {
      routeRequest('del', null, configuration, callback);
    },

    /**
     * reconf
     * 可动态更新 gid, hash, nodes
     */
    reconf: (configuration, callback) => {
      callback = callback || function(e, v) {
        if (e) console.error(e);
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

module.exports = mem;
