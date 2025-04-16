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
  
  // console.log('[routes.get] Received request for service:', service, 'in group:', gid);
  // console.log('[routes.get] Current local routes keys:', Object.keys(routes));

  let result;
  if (gid === "local" && (service in routes)) {
    // console.log('[routes.get] Found service in local routes:', service);
    result = routes[service];
  } 
  else if (global.distribution &&
           global.distribution[gid] &&
           (service in global.distribution[gid])) {
    // console.log('[routes.get] Found service in global.distribution:', gid, service);
    // console.log('[routes.get] Available services in group', gid, 'are:', Object.keys(global.distribution[gid]));
    result = global.distribution[gid][service];
  }
  else if (global.toLocal && global.toLocal[service]) {
    // console.log('[routes.get] Found service in global.toLocal:', service);
    result = { call: global.toLocal[service] };
  }
  
  if (!result) {
    // console.error(`[routes.get] Could not find service "${service}" in group "${gid}"`);
    if (global.distribution && global.distribution[gid]) {
      console.error('[routes.get] group object keys are:', Object.keys(global.distribution[gid]));
    } else {
      console.error('[routes.get] group object does not exist or is empty for gid:', gid);
    }
    return callback(new Error(`service "${service}" not found in group "${gid}"`), null);
  }

  // console.log('[routes.get] Returning service object for service:', service, 'group:', gid);
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
