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

    /*

      A common pattern in handling HTTP requests in Node.js is to have a
      subroutine that collects all the data chunks belonging to the same
      request. These chunks are aggregated into a body variable.

      When the req.on('end') event is emitted, it signifies that all data from
      the request has been received. Typically, this data is in the form of a
      string. To work with this data in a structured format, it is often parsed
      into a JSON object using JSON.parse(body), provided the data is in JSON
      format.

      Our nodes expect data in JSON format.
  */

    // Write some code...

    let body = [];
     
    req.on('data', (chunk) => {
      body.push(chunk) // when receive data
    });
 
    req.on('end', () => { // when finishing reciving data

      /* Here, you can handle the service requests.
=======
      /* Here, you can handle the service requests.
=======
<<<<<<< HEAD
<<<<<<< HEAD
      /* Here, you can handle the service requests.
=======
      /* Here, you can handle the service requests. 
>>>>>>> e962813 (Update from https://github.com/brown-cs1380/project/commit/142540f5b6b6295f9011be51733d3db4cc9a79cc)
=======
      /* Here, you can handle the service requests.
>>>>>>> 505414a (Update from https://github.com/brown-cs1380/project/commit/4489eea13d1ecd04998e6432f5074c619c87cc7f)
>>>>>>> 29afd06 (Update from https://github.com/brown-cs1380/project/commit/4489eea13d1ecd04998e6432f5074c619c87cc7f)
      Use the local routes service to get the service you need to call.
      You need to call the service with the method and arguments provided in the request.
      Then, you need to serialize the result and send it back to the caller.
      */

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
      routes.get(serviceName, (err, service) => {
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
