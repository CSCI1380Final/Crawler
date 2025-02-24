/* Notes/Tips:

- Use absolute paths to make sure they are agnostic to where your code is running from!
  Use the `path` module for that.
*/

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const baseDir = path.resolve(__dirname, 'store_data');

if (!fs.existsSync(baseDir)) {
  fs.mkdirSync(baseDir, { recursive: true });
}

function cleanFileName(key) {
  return key.replace(/[^a-zA-Z0-9]/g, '');
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
    let key = configuration;
    if (key == null) {
      const serialized = JSON.stringify(state);
      key = crypto.createHash('sha256').update(serialized).digest('hex');
      console.log(key);
    }
    key = cleanFileName(key);
    const filePath = path.join(baseDir, key);
    const data = JSON.stringify(state);
    fs.writeFile(filePath, data, 'utf8', (err) => {
      if (err) {
        callback(err, null);
      } else {
        callback(null, state);
      }
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
    let key = configuration;
    key = cleanFileName(key);
    const filePath = path.join(baseDir, key);
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) {
        callback(new Error('Not found'), null);
      } else {
        try {
          const value = JSON.parse(data);
          callback(null, value);
        } catch (e) {
          callback(e, null);
        }
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
    let key = configuration;
    key = cleanFileName(key);
    const filePath = path.join(baseDir, key);
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) {
        callback(new Error('Not found'), null);
      } else {
        let value;
        try {
          value = JSON.parse(data);
        } catch (e) {
          callback(e, null);
          return;
        }
        fs.unlink(filePath, (err2) => {
          if (err2) {
            callback(err2, null);
          } else {
            callback(null, value);
          }
        });
      }
    });
  } catch (error) {
    callback(error, null);
  }
}

module.exports = { put, get, del };
