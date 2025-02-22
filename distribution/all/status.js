const status = function(config) {
  const context = {};
  context.gid = config.gid || 'all';
  const distribution = global.distribution;

  return {
   get: (configuration, callback) => {
      if (typeof callback !== "function") {
        callback = function(err, result) {
          if (err) {
            console.error(err);
          } else {
            console.log(result);
          }
        };
      }
      global.distribution[context.gid].comm.send(
        [configuration],
        { service: "status", method: "get", gid: context.gid },
        function(err, result) {
          callback(err, result);
        }
      );
    },


   spawn: (configuration, callback) => {
        callback = callback || function(e, v) {
            if (e) {
            console.error(e)
            }else{
            console.log(v)
            }
      };
      distribution[context.gid].comm.send(
        [configuration],
        { service: "status", method: "spawn", gid: context.gid },
        (err, result) => {
          callback(err, result);
        }
      );
    },

    stop: (callback) => {
      callback = callback || function(e, v) {
        if (e) {
          console.error(e)
          }else{
          console.log(v)
          }
      };
      distribution[context.gid].comm.send(
        [],
        { service: "status", method: "stop", gid: context.gid },
        (err, result) => {
          callback(err, result);
        }
      );
    },
  };
};

module.exports = status;