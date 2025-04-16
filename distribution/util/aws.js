const distribution = require('../../config.js');

const n1 = { ip: '0.0.0.0', port: 8001 };

console.log("Starting AWS node...");


distribution.local.status.spawn(n1, (err, res) => {
  if (err) {
    console.error("Failed to spawn node:", err);
    process.exit(1);
  }
  console.log("Node spawned:", res);
  
  distribution.node.start((server) => {
    console.log("Server started on port", server.address().port);
     localServer = server;
    const ncdcConfig = {gid: 'ncdc'};
    startNodes(() => {
      distribution.local.groups.put(ncdcConfig, ncdcGroup, (e, v) => {
        distribution.ncdc.groups.put(ncdcConfig, ncdcGroup, (e, v) => {
          // 这里也保留对 avgwrdl / cfreq 的 put，不改
          const avgwrdlConfig = {gid: 'avgwrdl'};
          distribution.local.groups.put(avgwrdlConfig, avgwrdlGroup, (e, v) => {
            distribution.avgwrdl.groups.put(avgwrdlConfig, avgwrdlGroup, (e, v) => {
              const cfreqConfig = {gid: 'cfreq'};
              distribution.local.groups.put(cfreqConfig, cfreqGroup, (e, v) => {
                distribution.cfreq.groups.put(cfreqConfig, cfreqGroup, (e, v) => {
                  done();
                });
              });
            });
          });
        });
      });
    });
  });
});

