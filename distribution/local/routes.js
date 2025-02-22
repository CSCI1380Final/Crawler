/** @typedef {import("../types").Callback} Callback */
const routes = {};

/**
 * @param {string} configuration
 * @param {Callback} callback
 * @return {void}
 */

function get(configuration, callback) {
  callback = callback || function(e, v) {
    if (e) {
      console.error(e);
    } else {
      console.log(v);
    }
  };

  let service, gid;
  if (configuration instanceof Object) {
    service = configuration.service;
    gid = configuration.gid || "local";
  } else {
    service = configuration;
    gid = "local";
  }
  
  let result;
  // check local first
  if (gid === "local" && service in routes) {
    if (service in routes) {
      result = routes[service];
    }
  } 
  // nonlocal,  global.distribution
  else if (global.distribution &&
           global.distribution[gid] &&
           service in global.distribution[gid]) {
    result = global.distribution[gid][service];
  }
  
  // does not exist in your routes and call the corresponding RPC if that exists
  else if (global.toLocal[service]) {
    result = { call: global.toLocal[service] };
  }
  
  if (!result) {
    return callback(new Error(`service "${service}" not found in group "${gid}"`), null);
  }
  callback(null, result);
  return result;
}



/**
 * @param {object} service
 * @param {string} configuration
 * @param {Callback} callback
 * @return {void}
 */
function put(service, configuration, callback) {
   callback = callback || function(e, v) {
        if (e) {
        console.error(e)
        }else{
        console.log(v)
        }
   };
  if (typeof configuration !== "string") {
    return callback(new Error("Service must be a string"), null);
  }
  if (typeof service !== "object" || service === null) {
    return callback(new Error("Service must be an object"), null);
  }
  routes[configuration] = service;
  return callback(null, configuration); 
}

/**
 * @param {string} configuration
 * @param {Callback} callback
 */
function rem(configuration, callback) {
    callback = callback || function(e, v) {
        if (e) {
        console.error(e)
        }else{
        console.log(v)
        }
   };
  if (typeof configuration !== "string") {
    return callback(new Error("Service configuration must be a string"), null);
  }

  if (!(configuration in routes)) {
    return callback(new Error(`Service "${configuration}" not found`), null);
  }

  delete routes[configuration];
  return callback(null, configuration); 
}

module.exports = {get, put, rem};
