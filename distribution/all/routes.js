
/** @typedef {import("../types").Callback} Callback */

function routes(config) {
  const context = {};
  context.gid = config.gid || 'all';

  /**
   * @param {object} service
   * @param {string} name
   * @param {Callback} callback
   */
  function put(service, name, callback = () => { }) {
    callback = callback || function(e, v) {
        if (e) {
          console.error(e)
          }else{
          console.log(v)
          }
      };
    distribution[context.gid].comm.send([service, name],
      { service: "routes", method: "put", gid: context.gid }, 
      callback);
  }

  /**
   * @param {object} service
   * @param {string} name
   * @param {Callback} callback
   */
  function rem(service, name, callback = () => { }) {
     callback = callback || function(e, v) {
        if (e) {
          console.error(e)
          }else{
          console.log(v)
          }
      };
      distribution[context.gid].comm.send([service, name],
        { service: "routes", method: "rem", gid: context.gid }, 
        callback);
  }

  return {put, rem};
}

module.exports = routes;
