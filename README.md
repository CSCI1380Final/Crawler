# distribution

This is the distribution library. When loaded, distribution introduces functionality supporting the distributed execution of programs. To download it:

## Installation

```sh
$ npm i '@brown-ds/distribution'
```

This command downloads and installs the distribution library.

## Testing

There are several categories of tests:
  *	Regular Tests (`*.test.js`)
  *	Scenario Tests (`*.scenario.js`)
  *	Extra Credit Tests (`*.extra.test.js`)
  * Student Tests (`*.student.test.js`) - inside `test/test-student`

### Running Tests

By default, all regular tests are run. Use the options below to run different sets of tests:

1. Run all regular tests (default): `$ npm test` or `$ npm test -- -t`
2. Run scenario tests: `$ npm test -- -c` 
3. Run extra credit tests: `$ npm test -- -ec`
4. Run the `non-distribution` tests: `$ npm test -- -nd`
5. Combine options: `$ npm test -- -c -ec -nd -t`

## Usage

To import the library, be it in a JavaScript file or on the interactive console, run:

```js
let distribution = require("@brown-ds/distribution");
```

Now you have access to the full distribution library. You can start off by serializing some values. 

```js
let s = distribution.util.serialize(1); // '{"type":"number","value":"1"}'
let n = distribution.util.deserialize(s); // 1
```

You can inspect information about the current node (for example its `sid`) by running:

```js
distribution.local.status.get('sid', console.log); // 8cf1b
```

You can also store and retrieve values from the local memory:

```js
distribution.local.mem.put({name: 'nikos'}, 'key', console.log); // {name: 'nikos'}
distribution.local.mem.get('key', console.log); // {name: 'nikos'}
```

You can also spawn a new node:

```js
let node = { ip: '127.0.0.1', port: 8080 };
distribution.local.status.spawn(node, console.log);
```

Using the `distribution.all` set of services will allow you to act 
on the full set of nodes created as if they were a single one.

```js
distribution.all.status.get('sid', console.log); // { '8cf1b': '8cf1b', '8cf1c': '8cf1c' }
```

You can also send messages to other nodes:

```js
distribution.all.comm.send(['sid'], {node: node, service: 'status', method: 'get'}, console.log); // 8cf1c
```

# Results and Reflections

> ...

# M1: Serialization / Deserialization
## Summary
> Summarize your implementation, including key challenges you encountered. Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M1 (`hours`) and the lines of code per task.
For m1, I have completed the serialilze and deserialize interfaces. The 
serialize convert different type of object into Json string, and keep the meta data of the object after conversion. The deserialize restore the Json string into original object with its value. These two interfaces can be useful when transferring data through disk, network. One of the key challenges was to dealing with complex data structure, and I have fixed it by using recursive calls.
My implementation comprises 1 software components, totaling 270 lines of code with tests. Key challenges included dealing with complex data structure,.
## Correctness & Performance Characterization
> Describe how you characterized the correctness and performance of your implementation
*Correctness*: I wrote 5 tests; these tests take  to execute. This includes objects with 1.475s.
*Performance*: The latency of various subsystems is described in the `"latency"` portion of package.json. The characteristics of my development machines are summarized in the `"dev"` portion of package.json.


# M2: Actors and Remote Procedure Calls (RPC)


## Summary

> Summarize your implementation, including key challenges you encountered. Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M2 (`hours`) and the lines of code per task.

- I have complete the status service, allow request to inspct the information of the current node

- I have complete the routes service, allow to register any service with its method from local or through RPC

- I have complete the communication service, allow to send RPC through HTTP protocal. The function's argument are passed using the message array. It serialize the request during sending and deserialize the response when receiving.


My implementation comprises `3` software components, totaling `300` lines of code. Key challenges included `<1, 2, 3 + how you solved them>`.

Key challenges including passing the argument to RPC. I have figured out by checking the documents and find out the argument are from message array.


## Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation


*Correctness*: I wrote `8` tests in student tests; these tests take `0.878s` to execute.


*Performance*: I characterized the performance of comm and RPC by sending 1000 service requests in a tight loop. Average throughput and latency is recorded in `package.json`.


## Key Feature

> How would you explain the implementation of `createRPC` to someone who has no background in computer science — i.e., with the minimum jargon possible?

If there are two computer, computer 1 and computer 2. Computer 2 has 
much better resources and we would want to execute our tasks at computer 2.
Therefore, we have registered our tasks at computer 2. We then only need to send the message with tasks parameter from computer 1 to execute the the tasks at computer 2, and computer 2 will send back the results to computer 1.


# M3: Node Groups & Gossip Protocols

## Summary

> Summarize your implementation, including key challenges you encountered. Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M3 (`hours`) and the lines of code per task.

- I have complete local groups service. Through this, I have maintainined a group identifier (GID) to a set of nodes. Each node contains two groups by default, local and all. 

- I have expanded the comm.send and routes.get methods to support passing in an optional gid parameter, so as to construct an HTTP request can be send to a group from local perspective

- I implemented a distributed comm.send method that sends requests to all members of a node group in parallel.

- I have done distributed status.get request to all nodes in the group through distributed call, and each node returns its own status information

- I extended the local groups and routes services to make them distributed,  which is helpful to
sync the info for a global update

My implementation comprises `5` new software components, totaling `300` added lines of code over the previous implementation. Key challenges included `<1, 2, 3 + how you solved them>`.

- Key challenge is to completing local routes.get. There are many errors have happened. One scenario is to call the corresponding RPC if that exists if the service does not exist in local routes


## Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation


*Correctness* -- number of tests and time they take.

I have passed all of the 6 tests section except the last scenario gossip test. It 
takes average 7 s for each test section. 

*Performance* -- spawn times (all students) and gossip (lab/ec-only).

It takes about 4 seconds to spawn 6 nodes.


## Key Feature

> What is the point of having a gossip protocol? Why doesn't a node just send the message to _all_ other nodes in its group?

It prevent the cost of sending message for the source node to send message to all node. Using gossip protocal only require us to send log n number of nodes, and each nodes will pick another log n number to spread the message. The key is that this sending work is distributed across all nodes, rather than a single node taking the entire load.

# M4: Distributed Storage


## Summary

> Summarize your implementation, including key challenges you encountered

- I have complete the local store for memory and disk for operation put, get, delete

- I have complete the distributed store for memory and disk for operation put, get, delete that 
call local store service on each node

- I have done extra credits to handle get operation with key is a null

- I have done the consistent hash function and rendezvousHash hash function

- I hvae measured throughput and latency for put and get operation locallly and on cloud env

- The key challenges I have encountered is to handle the scenario when key is a null


Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M4 (`hours`) and the lines of code per task.


## Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation

I have write a file called aws.js to start my server on aws. I then record the ip and port of the server, using another aws node to send request and measure the throughput and latency. I repeat this process locally, and then record my measurement on package.json.

*Correctness* -- number of tests and time they take.

I have passed all test except the last reconf scenario test. 

The average time the test taken is 3s

*Performance* -- insertion and retrieval.

The get operation locally has average latency 3.20/ms and put operation locally has average latency 3.77/ms. 

The get operation on aws has average latency 1.16/ms and put operation on aws has average latency 1.66/ms. 

## Key Feature

> Why is the `reconf` method designed to first identify all the keys to be relocated and then relocate individual objects instead of fetching all the objects immediately and then pushing them to their corresponding locations?

If all objects are taken out first and then rewritten, many objects do not need to be migrated. After identifying the keys that need to be migrated, only these objects are migrated, thus reducing the amount of data transferred and the workload.
