/*
    In this file, add your own test cases that correspond to functionality introduced for each milestone.
    You should fill out each test case so it adequately tests the functionality you implemented.
    You are left to decide what the complexity of each test case should be, but trivial test cases that abuse this flexibility might be subject to deductions.

    Imporant: Do not modify any of the test headers (i.e., the test('header', ...) part). Doing so will result in grading penalties.
*/

const distribution = require('../../config.js');

const local = distribution.local;
const routes = distribution.local.routes;

test('(1 pts) student test', (done) => {
  // Fill out this test case...
    const otherService = {};

    otherService.calculate = () => {
      return [1,2,3];
    };

    routes.put(otherService, 'other', (e, v) => {
      routes.get('other', (e, v) => {
        try {
          expect(e).toBeFalsy();
          expect(v.calculate()).toEqual([1, 2, 3]);
          done();
        } catch (error) {
          done(error);
        }
      });
    });
});


test('(1 pts) student test', (done) => {
  // Fill out this test case...
    const node = distribution.node.config;
    const otherService = {};

    otherService.calculate = (x, callback) => {
      callback(null, x + 1);
    };
    // add calculate service
    routes.put(otherService, 'calculate')
    const remote = {node: node, service: 'calculate', method: 'calculate'};
    const message = [
      10,
    ];

    local.comm.send(message, remote, (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toEqual(11);
        done();
      } catch (error) {
        done(error);
      }
    });
});


test('(1 pts) student test', (done) => {
  // Fill out this test case...
  const node = distribution.node.config;
    const otherService = {};

    otherService.calculate = (x, y, callback) => {
      callback(null, x + y);
    };
    // add calculate service
    routes.put(otherService, 'calculate')
    const remote = {node: node, service: 'calculate', method: 'calculate'};
    const message = [
      10, 2
    ];

    local.comm.send(message, remote, (e, v) => {
      try {
        expect(e).toBeFalsy();
        expect(v).toEqual(12);
        done();
      } catch (error) {
        done(error);
      }
    });
});

test('(1 pts) student test', (done) => {
  // Fill out this test case...
  const node = distribution.node.config;
    const otherService = {};

    otherService.calculate = (callback) => {
      callback(new Error("test4"), null);
    };
    // add calculate service
    routes.put(otherService, 'calculate')
    const remote = {node: node, service: 'calculate', method: 'calculate'};
    const message = [];

    local.comm.send(message, remote, (e, v) => {
      try {
        expect(e).toBeTruthy(); 
        expect(e).toBeInstanceOf(Error); 
        expect(v).toBeFalsy(); 
        done();
      } catch (error) {
        done(error);
      }
    });
});

test('(1 pts) student test', (done) => {
  // Fill out this test case...
  const node = distribution.node.config;
    const otherService = {};

    otherService.calculate = (callback) => {
      callback(new Error("test5"), null);
    };
    // add calculate service
    routes.put(otherService, 'calculate')
    const remote = {node: node, service: 'calculate', method: 'calculate'};
    const message = [];
    // test error message
    local.comm.send(message, remote, (e, v) => {
      try {
        expect(e).toBeTruthy(); 
        expect(e).toBeInstanceOf(Error); 
        expect(e.message).toBe("test5");
        expect(v).toBeFalsy(); 
        done();
      } catch (error) {
        done(error);
      }
    });
});

let localServer = null;

beforeAll((done) => {
  distribution.node.start((server) => {
    localServer = server;
    done();
  });
});

afterAll((done) => {
  localServer.close();
  done();
});