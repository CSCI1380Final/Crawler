const http = require('http');
const url = require('url');
const log = require('../util/log');
const { serialize, deserialize } = require("@brown-ds/distribution/distribution/util/util");
const routes = require('./routes');

/*
    The start function will be called to start your node.
    It will take a callback as an argument.
    After your node has booted, you should call the callback.
*/


const start = function(callback) {
  const server = http.createServer((req, res) => {
    /* Your server will be listening for PUT requests. */

    // Write some code...
    // 405 status is method not allowed code
    if (req.method !== 'PUT') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      return res.end(serialize(new Error("Not put method")));
    }


    /*
      The path of the http request will determine the service to be used.
      The url will have the form: http://node_ip:node_port/service/method
    */


    // Write some code...
      const parsedUrl = url.parse(req.url, true);
      const path = parsedUrl.pathname.split('/').filter(Boolean);
      if (path.length < 3) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(serialize(new Error("Invalid path")));
      }
      const gid = path[0]
      const serviceName = path[1]
      const methodName = path[2]

    let body = [];
     
    req.on('data', (chunk) => {
      body.push(chunk) // when receive data
    });
 
    req.on('end', () => { 
      // Write some code...
      let requestData;
      try {
        body = Buffer.concat(body).toString();
        requestData = deserialize(body); 
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(serialize(err));
      }
       if (!Array.isArray(requestData)) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(serialize(err));
      }
      routes.get({ service: serviceName, gid: gid }, (err, service) => {
        if (err || !service || typeof service[methodName] !== 'function') {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          return res.end(serialize(err));
        }
        // call the service method
        service[methodName](...requestData, (error, result) => {
          if (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            return res.end(serialize(error));
          }

          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(serialize(result)); 
        });
      });

    });
  });


  /*
    Your server will be listening on the port and ip specified in the config
    You'll be calling the `callback` callback when your server has successfully
    started.

    At some point, we'll be adding the ability to stop a node
    remotely through the service interface.
  */

  server.listen(global.nodeConfig.port, global.nodeConfig.ip, () => {
    log(`Server running at http://${global.nodeConfig.ip}:${global.nodeConfig.port}/`);
    global.distribution.node.server = server;
    callback(server);
  });

  server.on('error', (error) => {
    // server.close();
    log(`Server error: ${error}`);
    throw error;
  });
};

module.exports = {
  start: start,
};
