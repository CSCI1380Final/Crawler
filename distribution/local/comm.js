/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../types").Node} Node */
const http = require('http');
const { serialize, deserialize } = require('../util/serialization');
const log = require("../util/log");

/**
 * @typedef {Object} Target
 * @property {string} service
 * @property {string} method
 * @property {Node} node
 */

/**
 * 
 * @param {Array} message 
 * @param {Target} remote 
 * @param {Callback} [callback] 
 */
function send(message, remote, callback) {
  callback = callback || function(e, v) {
    if (e) {
      console.error(e)
    }else{
      console.log(v)
    }
   };
  let data;
  try {
    data = serialize(message);
  } catch (error) {
    return callback(error);
  }
  let { node: { ip, port }, service, method } = remote;
  let gid = remote.gid || "local";
  
  const urlPath = `/${ip}:${port}/${gid}/${service}/${method}`;
  
//   log("options are: " + JSON.stringify({ ip, port, urlPath }) + " data: " + data);
  
  const options = {
    hostname: ip,
    port: port,
    method: 'PUT',
    path: urlPath,
    headers: {
      'Content-Type': 'application/json'
    }
  };
  
  const req = http.request(options, (res) => {
    let chunks = [];
    res.on("data", (chunk) => {
      chunks.push(chunk);
    });
    res.on("end", () => {
      let responseStr = Buffer.concat(chunks).toString();
      let responseObj;
      try {
        responseObj = deserialize(responseStr);
      } catch (error) {
        return callback(new Error("Error during deserialization"));
      }
      log("response: " + serialize(responseObj));
      if (res.statusCode !== 200) {
        return callback(new Error(`Request failed: ${JSON.stringify(responseObj.error)}`), responseObj.data);
      } else {
        return callback(responseObj.error, responseObj.data);
      }
    });
    res.on("error", (err) => {
      console.error(err);
    });
  });
  
  req.on("error", (err) => {
    callback(err);
  });
  req.write(data);
  req.end();
}

module.exports = { send };
