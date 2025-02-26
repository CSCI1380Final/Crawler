/* Notes/Tips:

- Use absolute paths to make sure they are agnostic to where your code is running from!
  Use the `path` module for that.
*/

const fs = require('fs');
const path = require('path');
const { id } = global.distribution.util;  

const baseDir = path.resolve(__dirname, 'store_data');
if (!fs.existsSync(baseDir)) {
  fs.mkdirSync(baseDir, { recursive: true });
}

/**
 * prevent illegal char
 */


function getEffectiveFileName(configuration, state) {
  if (configuration == null) {
    const autoKey = id.getID(state);
    return `local:${autoKey}`;
  }
  if (typeof configuration === 'object') {
    if (!configuration.key) {
      throw new Error("Configuration object must contain a 'key' property");
    }
    const gid = configuration.gid || "local";
    return `${gid}:${configuration.key}`;
  }
  return `local:${configuration}`;
}

function put(state, configuration, callback) {
  callback = callback || function(e, v) {
    if (e) {
      console.error(e);
    } else {
      console.log(v);
    }
  };

  try {
    const fileName = getEffectiveFileName(configuration, state);
    const filePath = path.join(baseDir, fileName);
    const data = JSON.stringify(state);
    fs.writeFile(filePath, data, 'utf8', (err) => {
      if (err) {
        return callback(err, null);
      }
      callback(null, state);
    });
  } catch (error) {
    callback(error, null);
  }
}

function get(configuration, callback) {
  callback = callback || function(e, v) {
    if (e) {
      console.error(e);
    } else {
      console.log(v);
    }
  };

  try {
    // 仅当 configuration 是对象且其 key 为 null时，认为这是请求全组所有 key
    if (typeof configuration === 'object' && configuration.key == null) {
      const gid = configuration.gid || "local";
      const prefix = gid + ":";
      console.log("prefix is", prefix);
      fs.readdir(baseDir, (err, files) => {
        if (err) return callback(err, null);
        // 只返回以当前组前缀开头的文件
        const filtered = files.filter(file => file.startsWith(prefix));
        // 剥离前缀，返回实际的 key
        const keys = filtered.map(file => file.slice(prefix.length));
        console.log("keys are", keys);
        return callback(null, keys);
      });
      return;
    }
    // 如果 configuration 是字符串，则直接使用它生成文件名
    const fileName = getEffectiveFileName(configuration);
    const filePath = path.join(baseDir, fileName);
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) {
        return callback(new Error('Not found'), null);
      }
      try {
        const value = JSON.parse(data);
        callback(null, value);
      } catch (parseErr) {
        callback(parseErr, null);
      }
    });
  } catch (error) {
    callback(error, null);
  }
}

function del(configuration, callback) {
  callback = callback || function(e, v) {
    if (e) {
      console.error(e);
    } else {
      console.log(v);
    }
  };

  try {
    const fileName = getEffectiveFileName(configuration);
    const filePath = path.join(baseDir, fileName);
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) {
        return callback(new Error('Not found'), null);
      }
      let value;
      try {
        value = JSON.parse(data);
      } catch (parseErr) {
        return callback(parseErr, null);
      }
      fs.unlink(filePath, (unlinkErr) => {
        if (unlinkErr) {
          return callback(unlinkErr, null);
        }
        callback(null, value);
      });
    });
  } catch (error) {
    callback(error, null);
  }
}

module.exports = { put, get, del };