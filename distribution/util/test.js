const serialization = require('./serialization');


const add2 = (t) => t+10

let res = serialization.serialize({a: 1, b: 2, c: 3})
console.log(res)
console.log(typeof(res))
// let parsed = JSON.parse(res)
// console.log(parsed.value)

deRes = serialization.deserialize(res)
console.log(deRes)


function add(a){
    return a + 1
}
 
