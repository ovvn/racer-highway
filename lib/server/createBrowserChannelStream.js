var Duplex = require('stream').Duplex;
var util = require('util');

module.exports = createBrowserChannelStream;

/**
 * @param {EventEmitters} client is a browserchannel client session for a given
 * browser window/tab that has a connection
 * @return {Duplex} stream
 */
function createBrowserChannelStream(client) {
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
  Duplex.call(this, {objectMode: true});

  this.client = client;

  var self = this;

  this.on('error', function(error) {
    console.warn('BrowserChannel client message stream error', error);
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
  if (this.client.state === 'closed') {
    return callback(new Error('WebSocket is not open'));
  }

  try {
    this.client.send(chunk);
    callback();
  } catch (err) {
    console.error('[racer-highway] BrowserChannel send error:', err);
    callback(err);
  }
};

ClientStream.prototype._stopClient = function() {
  let client = this.client;
  if (!client) return;

  this.client = null;

  try {
    if (typeof client.stop === 'function') {
      client.stop();
    } else if (typeof client.close === 'function') {
      client.close();
    }
  } catch (err) {
    console.error('[racer-highway] BC stop error:', err);
  }

  if (typeof this.push === 'function') this.push(null);
  this.writable = false;
  this.allowHalfOpen = false;
};
