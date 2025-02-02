const serialization = require('./serialization');


let res = serialization.serialize("strindqg")
console.log(res)
console.log(typeof(res))
deRes = serialization.deserialize(res)
console.log(deRes)


function add(){
    return 1
}

const add2 = () =>{
    return 2
}

console.log(add.toString())
console.log(add2.toString())