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
  if (typeof configuration !== "string") {
    return callback(new Error("Service must be a string"), null);
  }
  if (!(configuration in routes)) {
    return callback(new Error(`Service "${configuration}" is not found`), null);
  }

  return callback(null, routes[configuration]);
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
