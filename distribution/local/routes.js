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
        console.error(e)
        }else{
        console.log(v)
        }
   };
    let service
    let gid
    if (configuration instanceof Object) {
      service = configuration.service;
      gid = configuration.gid || "local"; // by default it should consider local
    } else {
      service = configuration;
      gid = "local";
    }
    let calledService;
  // forward to local
  if (gid === "local") {
    if (service in routes) {
      calledService = routes[service];
    }
  // forward to other nodes
  } else {
    if (
      global.distribution &&
      global.distribution[gid] &&
      service in global.distribution[gid]
    ) {
      calledService = global.distribution[gid][service];
    }
  }
  if (!calledService) {
    return callback(
      new Error(`service "${service}" not found in group "${gid}"`),
      null
    );
  }
  return callback(null, calledService);
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
