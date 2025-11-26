var Duplex = require('stream').Duplex;
var util = require('util');
var WebSocket = require('ws');

module.exports = createWebSocketStream;

/**
 * @param {EventEmitters} client is a WebSocket client session for a given
 * browser window/tab that has a connection
 * @return {Duplex} stream
 */
function createWebSocketStream(client) {
  var stream = new ClientStream(client);

  function onMessage(message) {
    var data;
    try {
      data = JSON.parse(message);
    } catch (err) {
      stream.emit('error', err);
      return;
    }
    stream.push(data);
  }

  function onClose() {
    // Signal data writing is complete. Emits the 'end' event
    stream.push(null);
  }

  client.on('message', onMessage);
  client.on('close', onClose);

  stream.on('close', function() {
    client.removeListener('message', onMessage);
    client.removeListener('close', onClose);
    if (client.timer) {
      clearInterval(client.timer);
      client.timer = null;
    }
  });

  return stream;
}

function ClientStream(client) {
  this.client = client;
  Duplex.call(this, {objectMode: true});

  var self = this;

  this.on('error', function(err) {
    console.warn('WebSocket client message stream error', err);
    // Ignore common connection resets to stop log spam
    // if (err.code !== 'EPIPE' && err.code !== 'ECONNRESET') {
    //  console.error('WebSocket client message stream error', err);
    // }
    self._stopClient();
  });

  // The server ended the writable stream. Triggered by calling stream.end()
  // in agent.close()
  this.on('finish', function() {
    self._stopClient();
  });
}
util.inherits(ClientStream, Duplex);

ClientStream.prototype._read = function() {};

ClientStream.prototype._write = function(chunk, encoding, callback) {
  if (this.client.readyState !== WebSocket.OPEN) {
    // We stop writing if socket is not open
    return callback(new Error('WebSocket is not open'));
  }
  try {
    this.client.send(JSON.stringify(chunk), function(err) {
      if (err) {
        console.error('[racer-highway] send:', err);
        // This stops ShareDB from sending more data.
        return callback(err);
      }
      callback();
    });

  } catch (error) {
    console.error('[racer-highway] send error:', error);
    // Propagate error to stop the stream
    callback(error);
  }
};

ClientStream.prototype._stopClient = function() {
  let client = this.client;
  try {
    client.close();
  } catch (error) {
    console.error('[racer-highway] Error while closing WebSocket in _stopClient:', error);
  }
};
