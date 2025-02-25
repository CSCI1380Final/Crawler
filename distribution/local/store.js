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
function cleanFileName(key) {
  return key.replace(/[^a-zA-Z0-9]/g, '');
}

function getEffectiveFileName(configuration, state) {
  if (configuration == null) {
    const autoKey = id.getID(state);

    return cleanFileName(`local:${autoKey}`);
  }
  if (typeof configuration === 'object') {
    //  { key, gid } 
    if (!configuration.key) {
      throw new Error("Configuration object must contain a 'key' property");
    }
    const gid = configuration.gid || 'local';
    const combinedKey = `${gid}:${configuration.key}`;
    return cleanFileName(combinedKey);
  }
  // result as  => "local:configuration" for groups differentiation
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
