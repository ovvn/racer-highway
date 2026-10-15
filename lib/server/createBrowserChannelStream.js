var Duplex = require('stream').Duplex;
var util = require('util');

module.exports = createBrowserChannelStream;

/**
 * @param {EventEmitters} client is a browserchannel client session for a given
 * browser window/tab that has a connection
 * @return {Duplex} stream
 */
function createBrowserChannelStream(client) {
  return new ClientStream(client);
}

// Like the WebSocket stream: ShareDB cleans up the agent only on 'end', so the
// stream is never destroyed and write callbacks never get an error.
function ClientStream(client) {
  Duplex.call(this, {objectMode: true});

  this.client = client;

  var self = this;

  this._onMessage = function(message) {
    var data;
    try {
      data = JSON.parse(message);
    } catch (err) {
      console.warn('[racer-highway] Invalid JSON from BrowserChannel client, stopping it:', err.message);
      return self._stopClient();
    }
    self.push(data);
  };
  this._onClose = function() {
    self._stopClient();
  };
  client.on('message', this._onMessage);
  client.on('close', this._onClose);

  // An 'error' without a listener would crash the process
  this.on('error', function(err) {
    if (err.code === 'ERR_STREAM_WRITE_AFTER_END' || err.code === 'ERR_STREAM_DESTROYED') return;
    console.warn('BrowserChannel client message stream error', err);
  });

  // The server ended the writable stream. Triggered by calling stream.end()
  // in agent.close()
  this.on('finish', function() {
    self._stopClient();
  });

  // Safety net: if Node destroys the stream anyway, make sure ShareDB still
  // sees 'end'
  this.on('close', function() {
    self._stopClient();
    if (!self.readableEnded) self.emit('end');
  });
}
util.inherits(ClientStream, Duplex);

ClientStream.prototype._read = function() {};

ClientStream.prototype._write = function(chunk, encoding, callback) {
  // Silently drop messages after the session is closed
  if (!this.client || this.client.state === 'closed') return callback();

  try {
    this.client.send(chunk);
  } catch (err) {
    console.error('[racer-highway] BrowserChannel send error:', err);
    this._stopClient();
  }
  callback();
};

ClientStream.prototype._stopClient = function() {
  var client = this.client;
  if (!client) return;
  this.client = null;

  client.removeListener('message', this._onMessage);
  client.removeListener('close', this._onClose);
  try {
    if (typeof client.stop === 'function') {
      client.stop();
    } else if (typeof client.close === 'function') {
      client.close();
    }
  } catch (err) {
    console.error('[racer-highway] BC stop error:', err);
  }

  // End the readable side: ShareDB cleans up the agent on 'end'
  this.push(null);
};
