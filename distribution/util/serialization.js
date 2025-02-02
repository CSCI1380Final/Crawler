
function serialize(object) {
  // Case null
  if (object === null) {
    return (
      JSON.stringify({type:"Null", value: "null" })
      )
  }
  type = typeof(object)
  // Case undefined
  if (type === "undefined") {
    return (
      JSON.stringify({type:"Undefined", value: "undefined"})
      )
  }
  // Case number
  if (type === "number") {
     return (
      JSON.stringify({type:"Number", value: object.toString()})
      )
  }
  // Case boolean
  if (type === "boolean") {
     return (
      JSON.stringify({type:"Boolean", value: object.toString() })
      )
  }
  // Case string
  if (type === "string") {
    return (
      JSON.stringify({type:"String", value: object})
    )
  }
  
}


function deserialize(string) {
  try{
  jsObject = JSON.parse(string);
  }catch(e){
    console.log(e)
    return null;
  }
  type = jsObject.type
  if (type === "Null") {
    return null;
  }
  if (type === "Undefined") {
    return undefined;
  }
  if (type === "Boolean") {
    return jsObject.value === "true";
  }
  if (type === "Number") {
    return Number(jsObject.value);
  }
  if (type === "String") {
    return jsObject.value;
  }
}

module.exports = {
  serialize: serialize,
  deserialize: deserialize,
};
