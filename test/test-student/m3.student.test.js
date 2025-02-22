/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../config.js');

test('(1 pts) student test', (done) => {
  // Fill out this test case...
  const g = {
    'a': {ip: '127.0.0.1', port: 8080},
    'b': {ip: '127.0.0.1', port: 8081},
  };

  distribution.local.groups.put('testgroup', g, (e, v) => {
    try {
      expect(e).toBeFalsy();
      expect(v).toBe(g);
      done();
    } catch (error) {
      done(error);
    }
  });
});

test('(1 pts) student test', (done) => {
  // Fill out this test case... 
  const gotchaService = {};

  gotchaService.gotcha = () => {
    return 1;
  };

  distribution.mygroup.routes.put(gotchaService,
      'calculate', (e, v) => {
        const n1 = {ip: '127.0.0.1', port: 8000};
        const n2 = {ip: '127.0.0.1', port: 8001};
        const n3 ={ip: '127.0.0.1', port: 8002};
        const r1 = {node: n1, service: 'routes', method: 'get'};
        const r2 = {node: n2, service: 'routes', method: 'get'};
        const r3 = {node: n3, service: 'routes', method: 'get'};

        distribution.local.comm.send(['calculate'], r1, (e, v) => {
          try {
            expect(e).toBeFalsy();
            expect(v.gotcha()).toBe(1);
          } catch (error) {
            done(error);
            return;
          }
          distribution.local.comm.send(['calculate'], r2, (e, v) => {
            try {
              expect(e).toBeFalsy();
              expect(v.gotcha()).toBe(1);
            } catch (error) {
              done(error);
              return;
            }
            distribution.local.comm.send(['calculate'], r3, (e, v) => {
              expect(e).toBeFalsy();
              try {
                expect(v.gotcha()).toBe(1);
                done();
              } catch (error) {
                done(error);
                return;
              }
            });
          });
        });
      });
});


test('(1 pts) student test', (done) => {
  // Fill out this test case...
    const g = {
    'a': {ip: '127.0.0.1', port: 8080},
    'b': {ip: '127.0.0.1', port: 8081},
  };

  distribution.local.groups.rem('testgroup', g, (e, v) => {
    try {
      expect(e).toBeInstanceOf(Error); 
      done();
    } catch (error) {
      done(error);
    }
  });
});

test('(1 pts) student test', (done) => {
  // Fill out this test case...
  const gotchaService = {};

  gotchaService.gotcha = () => {
    return "this is a test";
  };

  distribution.mygroup.routes.put(gotchaService,
      'calculate', (e, v) => {
        const n1 = {ip: '127.0.0.1', port: 8000};
        const n2 = {ip: '127.0.0.1', port: 8001};
        const n3 ={ip: '127.0.0.1', port: 8002};
        const r1 = {node: n1, service: 'routes', method: 'get'};
        const r2 = {node: n2, service: 'routes', method: 'get'};
        const r3 = {node: n3, service: 'routes', method: 'get'};

        distribution.local.comm.send(['calculate'], r1, (e, v) => {
          try {
            expect(e).toBeFalsy();
            expect(v.gotcha()).toBe("this is a test");
          } catch (error) {
            done(error);
            return;
          }
          distribution.local.comm.send(['calculate'], r2, (e, v) => {
            try {
              expect(e).toBeFalsy();
              expect(v.gotcha()).toBe("this is a test");
            } catch (error) {
              done(error);
              return;
            }
            distribution.local.comm.send(['calculate'], r3, (e, v) => {
              expect(e).toBeFalsy();
              try {
                expect(v.gotcha()).toBe("this is a test");
                done();
              } catch (error) {
                done(error);
                return;
              }
            });
          });
        });
      });
});

test('(1 pts) student test', (done) => {
  // Fill out this test case...
    const g = {
    'a': {ip: '127.0.0.1', port: 8080},
    'b': {ip: '127.0.0.1', port: 8081},
  };

  distribution.local.groups.put('testgroup', g, (e, v) => {
    distribution.local.groups.rem('testgroup', 'b', (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toBe('b');
        done();
      } catch (error) {
        done(error);
      }
    });
    })
});


const id = distribution.util.id;
/* Testing infrastructure code */

// This group is used for testing most of the functionality
const mygroupGroup = {};
// This group is used for {adding,removing} {groups,nodes}
const group4Group = {};

/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   This is because the process that node is
   running in is the actual jest process
*/
let localServer = null;

const n1 = {ip: '127.0.0.1', port: 8000};
const n2 = {ip: '127.0.0.1', port: 8001};
const n3 = {ip: '127.0.0.1', port: 8002};
const n4 = {ip: '127.0.0.1', port: 8003};
const n5 = {ip: '127.0.0.1', port: 8004};
const n6 = {ip: '127.0.0.1', port: 8005};


beforeAll((done) => {
  // First, stop the nodes if they are running
  const remote = {service: 'status', method: 'stop'};

  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n4;
        distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n5;
          distribution.local.comm.send([], remote, (e, v) => {
            remote.node = n6;
            distribution.local.comm.send([], remote, (e, v) => {
            });
          });
        });
      });
    });
  });

  mygroupGroup[id.getSID(n1)] = n1;
  mygroupGroup[id.getSID(n2)] = n2;
  mygroupGroup[id.getSID(n3)] = n3;

  group4Group[id.getSID(n1)] = n1;
  group4Group[id.getSID(n2)] = n2;
  group4Group[id.getSID(n4)] = n4;

  // Now, start the base listening node
  distribution.node.start((server) => {
    localServer = server;

    const groupInstantiation = (e, v) => {
      const mygroupConfig = {gid: 'mygroup'};
      const group4Config = {gid: 'group4'};

      // Create some groups
      distribution.local.groups
          .put(mygroupConfig, mygroupGroup, (e, v) => {
            distribution.local.groups
                .put(group4Config, group4Group, (e, v) => {
                  done();
                });
          });
    };

    // Start the nodes
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          distribution.local.status.spawn(n4, (e, v) => {
            distribution.local.status.spawn(n5, (e, v) => {
              distribution.local.status.spawn(n6, groupInstantiation);
            });
          });
        });
      });
    });
  });
});

afterAll((done) => {
  distribution.mygroup.status.stop((e, v) => {
    const remote = {service: 'status', method: 'stop'};
    remote.node = n1;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n2;
      distribution.local.comm.send([], remote, (e, v) => {
        remote.node = n3;
        distribution.local.comm.send([], remote, (e, v) => {
          remote.node = n4;
          distribution.local.comm.send([], remote, (e, v) => {
            remote.node = n5;
            distribution.local.comm.send([], remote, (e, v) => {
              remote.node = n6;
              distribution.local.comm.send([], remote, (e, v) => {
                localServer.close();
                done();
              });
            });
          });
        });
      });
    });
  });
});
