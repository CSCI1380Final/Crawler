const distribution = require('../../config.js');

const n1 = { ip: '127.0.0.1', port: 8001 };

console.log("Starting AWS node...");


distribution.local.status.spawn(n1, (err, res) => {
  if (err) {
    console.error("Failed to spawn node:", err);
    process.exit(1);
  }
  console.log("Node spawned:", res);
  
  distribution.node.start((server) => {
    console.log("Server started on port", server.address().port);
  });
});
