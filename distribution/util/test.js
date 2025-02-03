const serialization = require('./serialization');


const add2 = (t) => t+10

let res = serialization.serialize(add2)
console.log(res)
console.log(typeof(res))
deRes = serialization.deserialize(res)
console.log(deRes)


function add(a){
    return a + 1
}

console.log(deRes(10))
