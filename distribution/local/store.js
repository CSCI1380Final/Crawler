/* Notes/Tips:

- Use absolute paths to make sure they are agnostic to where your code is running from!
  Use the `path` module for that.
*/

const fs = require('fs');
const path = require('path');
const { id } = global.distribution.util;  

// 存储文件的根目录
const baseDir = path.resolve(__dirname, 'store_data');
if (!fs.existsSync(baseDir)) {
  fs.mkdirSync(baseDir, { recursive: true });
}

/**
 * 只保留字母数字，防止文件名非法字符
 */
function cleanFileName(key) {
  return key.replace(/[^a-zA-Z0-9]/g, '');
}

/**
 * 生成“有效文件名”的辅助函数
 * 如果 configuration == null，则根据 state 生成 sha256 作为 key；
 * 如果 configuration 是对象 { key, gid }，则组合成 "gid:key" 做文件名；
 * 否则把 configuration 当字符串，用 "local:configuration"。
 * 最后对组合结果执行 cleanFileName，以保证可用做文件名。
 */
function getEffectiveFileName(configuration, state) {
  if (configuration == null) {
    const autoKey = id.getID(state);
    
    return cleanFileName(`local:${autoKey}`);
  }
  if (typeof configuration === 'object') {
    // 需要 { key, gid } 属性
    if (!configuration.key) {
      throw new Error("Configuration object must contain a 'key' property");
    }
    const gid = configuration.gid || 'local';
    const combinedKey = `${gid}:${configuration.key}`;
    return cleanFileName(combinedKey);
  }
  // 否则视为字符串 => "local:configuration"
  const combinedKey = `local:${configuration}`;
  return cleanFileName(combinedKey);
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
    // 将 state 序列化后写入文件
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
