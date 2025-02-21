/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../types").Node} Node */
const http = require('http');
const { serialize, deserialize } = require('../util/serialization');

/**
 * @typedef {Object} Target
 * @property {string} service
 * @property {string} method
 * @property {Node} node
 */

/**
 * @param {Array} message
 * @param {Target} remote
 * @param {Callback} [callback]
 * @return {void}
 */
function send(message, remote, callback) {
    callback = callback || function(e, v) {
        if (e) {
        console.error(e)
        }else{
        console.log(v)
        }
    }
    if (!Array.isArray(message)) {
        return callback(new Error("Message is not an array"));
    }

    if (!remote || typeof remote !== "object") {
        return callback(new Error("Remote must be an object"));
    }
    if (!remote.node || !remote.service || !remote.method) {
        return callback(new Error("Invalid remote object."));
    }

     const targetNode = remote.node;

     const gid = remote.gid || "local";
     const urlPath = `/${gid}/${remote.service}/${remote.method}`;
    // serialize message as string
    const requestData = serialize(message); 

    const options = {
        hostname: targetNode.ip,
        port: targetNode.port,
        path: urlPath,
        method: 'PUT',
        headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(requestData),
        },
    };

    const req = http.request(options, (res) => {
        let responseData = '';
        res.on('data', (chunk) => {
        responseData += chunk;
        });

        res.on('end', () => {
        try {
            const jsonResponse = deserialize(responseData); 
            if (jsonResponse instanceof Error) {
                callback(jsonResponse, null);
            }else{
                callback(null, jsonResponse);
            }
        } catch (error) {
            callback(new Error("something wrong with deserializing"));
        }
        });
  });

    req.write(requestData);
    req.end();
}

module.exports = {send};
