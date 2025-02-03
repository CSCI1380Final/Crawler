
function serialize(object) {
  // Case null
  if (object === null) {
    return (
      JSON.stringify({type:"null", value: "null" })
      )
  }
  let type = typeof(object)
  // Case undefined
  if (type === "undefined") {
    return (
      JSON.stringify({type:"undefined", value: "undefined"})
      )
  }
  // Case number
  if (type === "number") {
     return (
      JSON.stringify({type:"number", value: object.toString()})
      )
  }
  // Case boolean
  if (type === "boolean") {
     return (
      JSON.stringify({type:"boolean", value: object.toString() })
      )
  }
  // Case string
  if (type === "string") {
    return (
      JSON.stringify({type:"string", value: object})
    )
  }
  if (type === "function") {
     return (
      JSON.stringify({type:"function", value: object.toString()})
    )
  }
   if (object instanceof Date) {
    return JSON.stringify({
      type: "date",
      value: object.toISOString() 
    });
  }
  if (object instanceof Error) {
    return JSON.stringify({
      type: "error",
      name: object.name,
      value: object.message,
    });
  }
  if (object instanceof Array) {
    const processed_arr = object.map((e)=> {
      return serialize(e)
     })
     return JSON.stringify({
        type: "array",
        value: processed_arr
      })
  }
  if (type === "object") {
    const serializedObj = {};
    for (const key in object) {
      if (object.hasOwnProperty(key)) {
        serializedObj[key] = serialize(object[key]);
      }
    }
    return JSON.stringify({
      type: "object",
      value: serializedObj
    });
  }
}


function deserialize(string) {
  let jsObject;
  try{
   jsObject = JSON.parse(string);
  }catch(e){
    console.log(e)
    return null;
  }
  let type = jsObject.type
  if (type === "null") {
    return null;
  }
  if (type === "undefined") {
    return undefined;
  }
  if (type === "boolean") {
    return jsObject.value === "true";
  }
  if (type === "number") {
    return Number(jsObject.value);
  }
  if (type === "string") {
    return jsObject.value;
  }
   if (type === "function") {
    return parseFunction(jsObject.value);
  }
  if (type === "date") {
    return new Date(jsObject.value)
  }
  if (type === "error") {
     const error = new Error(jsObject.value);
     error.name = jsObject.name;
     return error
  }
  if (type === "array") {
     const deserializeList = jsObject.value.map(e => deserialize(e))
     return deserializeList
  }
  if (type === "object") {
    const obj = {};
    for (const key in jsObject.value) {
      obj[key] = deserialize(jsObject.value[key]); 
    }
    return obj;
  }
  throw new SyntaxError();
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
