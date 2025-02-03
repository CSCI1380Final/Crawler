const serialization = require('./serialization');


const add2 = (t) => t+10

let res = serialization.serialize({a: 'jcarb', b: 1, c: (a, b) => a + b})
 const serializedObject = '{"type":"object","value":{"a":"{\\"type\\":\\"string\\",\\"value\\":\\"jcarb\\"}","b":"{\\"type\\":\\"number\\",\\"value\\":\\"1\\"}","c":"{\\"type\\":\\"function\\",\\"value\\":\\"(a, b) => a + b\\"}"}}';

console.log(res === serializedObject)
console.log(typeof(res))
// let parsed = JSON.parse(res)
// console.log(parsed.value)

deRes = serialization.deserialize(res)
console.log(deRes)


function add(a){
    return a + 1
}
 
