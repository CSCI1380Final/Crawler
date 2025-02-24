const crypto = require('crypto');

const store = {};

function put(state, configuration, callback) {
    callback = callback || function(e, v) {
        if (e) {
        console.error(e)
        }else{
        console.log(v)
        }
    };
    try {
        let key = configuration;
        // when key is null
        if (key == null) {
        key = crypto.createHash('sha256').update(JSON.stringify(state)).digest('hex');
        }
        store[key] = state;
        callback(null, state);
    } catch (error) {
        callback(error, null);
    }
};

function get(configuration, callback) {
    callback = callback || function(e, v) {
        if (e) {
        console.error(e)
        }else{
        console.log(v)
        }
    };
     if (configuration in store) {
        callback(null, store[configuration]);
    } else {
        callback(new Error('Not found in memory during get'), null);
    }
}

function del(configuration, callback) {
    callback = callback || function(e, v) {
        if (e) {
        console.error(e)
        }else{
        console.log(v)
        }
    };
    if (configuration in store) {
        const value = store[configuration];
        delete store[configuration];
        callback(null, value);
    } else {
        callback(new Error('Not found during delete'), null);
    }
};

module.exports = {put, get, del};
