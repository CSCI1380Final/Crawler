/** @typedef {import("../types").Callback} Callback */
/** @typedef {import("../types").Node} Node */
const http = require('http');
const { serialize, deserialize } = require('../util/serialization');
const log = require("../util/log");

// 启用 Keep-Alive
const agent = new http.Agent({ keepAlive: true });

/**
 * @typedef {Object} Target
 * @property {string} service
 * @property {string} method
 * @property {Node} node
 */

/**
 * 发送消息
 * @param {Array} message 
 * @param {Target} remote 
 * @param {Callback} [callback] 
 */
function send(message, remote, callback) {
  callback = callback || function(e, v) {
    if (e) {
      console.error(e);
    } else {
      // console.log(v);
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

  // 在发起请求前，打印调试信息
  console.log(`[DEBUG comm.send] Preparing request to ${ip}:${port} path="${urlPath}"`);
  console.log(`[DEBUG comm.send] Message length: ${data.length} bytes, Agent keepAlive=${agent.keepAlive}`);

  const options = {
    hostname: ip,
    port: port,
    method: 'PUT',
    path: urlPath,
    agent: agent,
    headers: {
      'Content-Type': 'application/json'
    }
  };

  const req = http.request(options, (res) => {
    console.log(`[DEBUG comm.send] Response from ${ip}:${port}, statusCode=${res.statusCode}`);

    let chunks = [];
    res.on("data", (chunk) => {
      chunks.push(chunk);
    });
    res.on("end", () => {
      let responseStr = Buffer.concat(chunks).toString();
      console.log(`[DEBUG comm.send] Full response body from ${ip}:${port}:`, responseStr);

      let responseObj;
      try {
        responseObj = deserialize(responseStr);
      } catch (error) {
        console.error(`[DEBUG comm.send] Deserialization error from ${ip}:${port}:`, error);
        return callback(new Error("Error during deserialization"));
      }

      // 打印一下反序列化后的对象
      log("response: " + serialize(responseObj));

      if (res.statusCode !== 200) {
        console.error(`[DEBUG comm.send] Non-200 status from ${ip}:${port}:`, responseObj.error);
        return callback(new Error(`Request failed: ${JSON.stringify(responseObj.error)}`), responseObj.data);
      } else {
        return callback(responseObj.error, responseObj.data);
      }
    });

    // 如果读取响应流本身时出错
    res.on("error", (err) => {
      console.error(`[DEBUG comm.send] Response stream error from ${ip}:${port}`, err);
    });
  });

  // 如果在发起请求或连接阶段出现错误
  req.on("error", (err) => {
    console.error(`[DEBUG comm.send] Request error to ${ip}:${port}`, err);
    callback(err);
  });

  // 写入请求主体
  req.write(data);
  req.end();
}

module.exports = { send };
