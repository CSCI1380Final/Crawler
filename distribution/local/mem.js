const { id } = global.distribution.util;  

const store = {};

function getEffectiveKey(configuration, state) {
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
    const effectiveKey = getEffectiveKey(configuration, state);
    store[effectiveKey] = state;
    callback(null, state);
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
    if (configuration == null) {
      const allKeys = Object.keys(store);
      const strippedKeys = allKeys.map(k => {
        const parts = k.split(':');
        return parts.length > 1 ? parts.slice(1).join(':') : k;
      });
      return callback(null, strippedKeys);
    }
    const effectiveKey = getEffectiveKey(configuration);
    if (effectiveKey in store) {
      callback(null, store[effectiveKey]);
    } else {
      callback(new Error('Not found in memory during get'), null);
    }
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
    const effectiveKey = getEffectiveKey(configuration);
    if (effectiveKey in store) {
      const value = store[effectiveKey];
      delete store[effectiveKey];
      callback(null, value);
    } else {
      callback(new Error('Not found during delete'), null);
    }
  } catch (error) {
    callback(error, null);
  }
}

module.exports = { put, get, del };
