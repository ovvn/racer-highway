var Duplex = require('stream').Duplex;
var util = require('util');
var WebSocket = require('ws');

module.exports = createWebSocketStream;

/**
 * @param {EventEmitters} client is a browserchannel client session for a given
 * browser window/tab that is has a connection
 * @return {Duplex} stream
 */
function createWebSocketStream(client) {
  var stream = new ClientStream(client);

  client.on('message', function onMessage(message) {
    var data;
    try {
      data = JSON.parse(message);
    } catch (err) {
      stream.emit('error', err);
      return;
    }
    stream.push(data);
  });

  client.on('close', function() {
    // Signal data writing is complete. Emits the 'end' event
    stream.push(null);
  });

  return stream;
}

function ClientStream(client) {
  this.client = client;
  Duplex.call(this, {objectMode: true});

  var self = this;

  this.on('error', function(error) {
    console.warn('WebSocket client message stream error', error);
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
  // Silently drop messages after the session is closed
  if (this.client.readyState !== WebSocket.OPEN) {
    return callback();
  }
  try {
    this.client.send(JSON.stringify(chunk), function(err) {
      if (err) {
        console.error('[racer-highway] send:', err);
      }
      callback();
    });

  } catch (error) {
    console.error('[racer-highway] send error:', error);
    callback();
  }
};

ClientStream.prototype._stopClient = function() {
  var client = this.client;
  client.close();
};
