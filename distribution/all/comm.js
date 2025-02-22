/** @typedef {import("../types").Callback} Callback */
const groups = require('../local/groups');
/**
 * NOTE: This Target is slightly different from local.all.Target
 * @typdef {Object} Target
 * @property {string} service
 * @property {string} method
 */

/**
 * @param {object} config
 * @return {object}
 */
function comm(config) {
  const context = {};
  context.gid = config.gid || 'all';

  /**
   * @param {Array} message
   * @param {object} configuration
   * @param {Callback} callback
   */
  function send(message, configuration, callback) {
     callback = callback || function(e, v) {
      if (e) {
        console.error(e)
      }else{
        console.log(v)
      }
    };
     const nodesMap = groups.get(context.gid, (e, v) => console.log("Group nodes:", v));
    const nodeIds = Object.keys(nodesMap);
    const total = nodeIds.length;

    if (total === 0) {
      return callback(null, { errors: {}, responses: {} });
    }

    let remaining = total;
    const errorMap = {};
    const responseMap = {};

    for (const nodeId of nodeIds) {
      const nodeInfo = nodesMap[nodeId];
      const remoteSpec = {
        node: nodeInfo,
        service: configuration.service,
        method: configuration.method
      };
      global.distribution.local.comm.send(message, remoteSpec, (err, res) => {
        if (err) {
          errorMap[nodeId] = err;
        } else {
          responseMap[nodeId] = res;
        }
        remaining--;
        if (remaining === 0) {
          callback(errorMap, responseMap);
        }
      });
    }
  }

  return {send};
};

module.exports = comm;