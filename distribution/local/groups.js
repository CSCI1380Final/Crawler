const { id } = require("@brown-ds/distribution/distribution/util/util");
const groups = {};

groups.get = function(name, callback) {
    callback = callback || function(e, v) {
        if (e) {
        console.error(e)
        }else{
        console.log(v)
        }
    }
    if (name in groups) {
        let value = groups[name];
        callback(undefined, value)
        return value // need to explictly return value for caller
    } else {
        let error = new Error(`${name} does not existed in group`);
        callback(error, undefined)
    }
};

groups.put = function(config, group, callback) {
  callback = callback || function(e, v) {
    if (e) {
      console.error(e);
    } else {
      console.log(v);
    }
  };

  let effectiveGid, effectiveHash;
  if (typeof config === 'object') {
    effectiveGid = config.gid || "local";
    effectiveHash = config.hash; 
  } else {
    effectiveGid = config;
  }

  global.distribution[effectiveGid] = {
    status: require('../all/status')({ gid: effectiveGid }),
    comm: require('../all/comm')({ gid: effectiveGid }),
    gossip: require('../all/gossip')({ gid: effectiveGid }),
    groups: require('../all/groups')({ gid: effectiveGid }),
    routes: require('../all/routes')({ gid: effectiveGid }),
    // mkae hash to mem/store
    mem: require('../all/mem')({ gid: effectiveGid, hash: effectiveHash }),
    store: require('../all/store')({ gid: effectiveGid, hash: effectiveHash }),
  };

  groups[effectiveGid] = group;
  callback(undefined, group);
};

groups.del = function(name, callback) {
    callback = callback || function(e, v) {
        if (e) {
        console.error(e)
        }else{
        console.log(v)
        }
    }
    if (name in groups) {
        let node_info = groups[name];
        delete groups[name];
        callback(undefined, node_info)
    }else{
        let error = new Error(`failed to delete group ${name}`);
        callback(error, undefined)
    }
};

groups.add = function(name, node, callback) {
   callback = callback || function(e, v) {
        if (e) {
        console.error(e)
        }else{
        console.log(v)
        }
    }
    if (!groups[name]) {
        groups[name] = {};
    }
    groups[name][id.getSID(node)] = node;
    callback(undefined, node);
};


groups.rem = function(name, node, callback) {
    callback = callback || function(e, v) {
        if (e) {
        console.error(e)
        }else{
        console.log(v)
        }
    }
    if (node in groups[name]) {
        delete groups[name][node];
        callback(undefined, node)
    }else{
        let error = new Error(`failed to delete ${name} in group`);
        callback(error, undefined)
    }
};

module.exports = groups;
