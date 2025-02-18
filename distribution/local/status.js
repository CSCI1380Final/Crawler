const log = require('../util/log');

const status = {};

global.moreStatus = {
  sid: global.distribution.util.id.getSID(global.nodeConfig),
  nid: global.distribution.util.id.getNID(global.nodeConfig),
  counts: 0,
};

status.get = function(configuration, callback) {
  callback = callback || function(e, v) {
    if (e) {
      console.error(e)
    }else{
      console.log(v)
    }
   };
  if (configuration === "heapTotal" || configuration === "heapUsed") {
    return callback(null, process.memoryUsage()[configuration]);
  }

  if (configuration === "nid") return callback(null, global.moreStatus.nid);
  if (configuration === "sid") return callback(null, global.moreStatus.sid);
  if (configuration === "counts") return callback(null, global.moreStatus.counts);
  if (configuration === "ip") return callback(null, global.nodeConfig.ip);
  if (configuration === "port") return callback(null, global.nodeConfig.port);

  return callback(new Error(`Error status configuration: ${configuration}`), null);
};


status.spawn = function(configuration, callback) {
};

status.stop = function(callback) {
};

module.exports = status;
