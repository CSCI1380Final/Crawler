
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
  if (type === "function") {
     return (
      JSON.stringify({type:"Function", value: object.toString()})
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
   if (type === "Function") {
    return parseFunction(jsObject.value);
  }
}


// helper function for deal case of function
const parseFunction = (funcString) => {
  let args, body;
  // normal function string
  if (funcString.startsWith("function")) {
    const argsStart = funcString.indexOf("(") + 1;
    const argsEnd = funcString.indexOf(")");
    args = funcString.slice(argsStart, argsEnd).split(",").map(a => a.trim());
    const bodyStart = funcString.indexOf("{") + 1;
    const bodyEnd = funcString.lastIndexOf("}");
    body = funcString.slice(bodyStart, bodyEnd).trim();
  }
  // arrow function string
  else {
    const arrowIndex = funcString.indexOf("=>");
    const argsPart = funcString.slice(0, arrowIndex).trim();
    let bodyPart = funcString.slice(arrowIndex + 2).trim();

    if (argsPart.startsWith("(")) {
      args = argsPart.slice(1, -1).split(",").map(a => a.trim());
    } else {
      args = [argsPart]; 
    }
    // shortcut return function
    if (!bodyPart.startsWith("{")) {
      body = "return " + bodyPart + ";"
    } else {
      body = bodyPart.slice(1, -1).trim();
    }
  }
  return new Function(...args, body);
}

module.exports = {
  serialize: serialize,
  deserialize: deserialize,
};
