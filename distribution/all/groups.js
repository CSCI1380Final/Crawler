const groups = function(config) {
  const context = {};
  context.gid = config.gid || 'all';
  const distribution = global.distribution; // Use global.distribution to access initialized services in the entire distributed system.

  return {
    put: (cfg, group, callback) => {
      callback = callback || function(e, v) {
        if (e) console.error(e);
        else console.log(v);
      };

      let effectiveCfg;
      if (typeof cfg === 'string') {
        effectiveCfg = { gid: cfg };
      } else if (typeof cfg === 'object') {
        effectiveCfg = {
          gid: cfg.gid || context.gid,
          hash: cfg.hash, 
        };
      } else {
        effectiveCfg = { gid: context.gid };
      }
      
      distribution[context.gid].comm.send(
        [effectiveCfg, group],
        { service: "groups", method: "put", gid: context.gid },
        callback
      );
    },
    
    del: (name, callback) => {
      distribution[context.gid].comm.send(
        [name],
        { service: "groups", method: "del", gid: context.gid },
        callback
      );
    },

    get: (name, callback) => {
      distribution[context.gid].comm.send(
        [name],
        { service: "groups", method: "get", gid: context.gid },
        callback
      );
    },

    add: (name, node, callback) => {
      distribution[context.gid].comm.send(
        [name, node],
        { service: "groups", method: "add", gid: context.gid },
        callback
      );
    },

    rem: (name, node, callback) => {
      distribution[context.gid].comm.send(
        [name, node],
        { service: "groups", method: "rem", gid: context.gid },
        callback
      );
    }
  };
};

module.exports = groups;
