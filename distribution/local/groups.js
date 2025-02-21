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
        callback(null, value)
    } else {
        let error = new Error(`${name} does not existed in group`);
        callback(error, null)
    }
};

groups.put = function(config, group, callback) {
    callback = callback || function(e, v) {
        if (e) {
        console.error(e)
        }else{
        console.log(v)
        }
    }
    let configuration = config;
    if (configuration instanceof Object) {
        configuration = config.gid;
    }
    global.distribution[configuration] = {
        status: require('../all/status')({gid: configuration}),
        comm: require('../all/comm')({gid: configuration}),
        gossip: require('../all/gossip')({gid: configuration}),
        groups: require('../all/groups')({gid: configuration}),
        routes: require('../all/routes')({gid: configuration}),
        mem: require('../all/mem')({gid: configuration}),
        store: require('../all/store')({gid: configuration}),
    };
    groups[configuration] = group;
    callback(null, group)
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
        callback(null, node_info)
    }else{
        let error = new Error(`failed to delete group ${name}`);
        callback(error, null)
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
    groups[name][id.getSID(node)] = node;
    callback(null, node)
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
        callback(null, node)
    }else{
        let error = new Error(`failed to delete ${name} in group`);
        callback(error, null)
    }
};

module.exports = groups;
