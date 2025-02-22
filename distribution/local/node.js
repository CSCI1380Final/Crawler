const http = require('http');
const url = require('url');
const log = require('../util/log');
const { serialize, deserialize } = require('../util/serialization');
const routes = require('./routes');

/*
    The start function will be called to start your node.
    It will take a callback as an argument.
    After your node has booted, you should call the callback.
*/

const start = function(callback = () => {}) {
  const server = http.createServer((req, res) => {
     /* Your server will be listening for PUT requests. */

    // Write some code...
    // 405 status is method not allowed code
    if (req.method !== "PUT") {
      res.writeHead(405, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ error: "Method Not Allowed" }));
    }

    const parsedUrl = url.parse(req.url);
    const urls = parsedUrl.pathname.split('/');

    if (urls.length < 5) {
      res.writeHead(400, { "Content-Type": "application/json" });
      return res.end(serialize(new Error("Invalid path")));
    }
    const gid = urls[2];
    const serviceName = urls[3];
    const serviceMethod = urls[4];

    let body = [];
    req.on("data", (chunk) => {
      body.push(chunk.toString());
    });

    req.on("end", () => {
      let params;
      try {
        const data = body.join('');
        params = deserialize(data);
      } catch (err) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(serialize(err));
      }
      if (!Array.isArray(params)) {
        res.writeHead(400, { "Content-Type": "application/json" });
        return res.end(serialize(new Error("Request data must be an array")));
      }

      // use local routes to get serviceObj
      const serviceConfig = { service: serviceName, gid: gid };
      let serviceObj;
      try {
        serviceObj = routes.get(serviceConfig);
        if (!serviceObj || !(serviceMethod in serviceObj)) {
          throw new Error(`Invalid method or service: ${serviceName}, ${serviceMethod}`);
        }
      } catch (error) {
        res.writeHead(404, { "Content-Type": "application/json" });
        return res.end(serialize(error));
      }

      // call service with parameter
      const method = serviceObj[serviceMethod];
      method(...params, (e, v) => {
        if (e instanceof Error) {
          res.writeHead(500, { "Content-Type": "application/json" });
          return res.end(serialize(e));
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(serialize({ data: v, error: e }));
      });
    });
  });

  server.listen(global.nodeConfig.port, global.nodeConfig.ip, () => {
    log(`Server running at http://${global.nodeConfig.ip}:${global.nodeConfig.port}/`);
    global.distribution.node.server = server;
    callback(server);
  });

  server.on("error", (error) => {
    log(`Server error: ${error}`);
    throw error;
  });
};

module.exports = {
  start: start,
};
