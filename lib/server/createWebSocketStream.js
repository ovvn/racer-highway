let Duplex = require('stream').Duplex;
let util = require('util');

let { WebSocket } = require('ws');
let describeClient = require('./describeClient');

module.exports = createWebSocketStream;

/**
 * @param {EventEmitters} client is a WebSocket client session for a given
 * browser window/tab that has a connection
 * @param options
 * @return {Duplex} stream
 */
function createWebSocketStream(client, options) {
  return new ClientStream(client, options);
}

// ShareDB cleans up an agent (subscriptions, query pollers, agentsCount) only
// when the stream emits 'end', and a destroyed stream never emits 'end'. So
// this stream is not destroyed on teardown, and write callbacks never get an
// error (Node destroys a stream on a write error). _stopClient() ends the
// readable side instead.
function ClientStream(client, options) {
  Duplex.call(this, {objectMode: true});

  options = options || {};
  this.client = client;
  this._bufferLimit = (options.wsMaxBufferMB || 5) * 1024 * 1024;
  this._bufferStallMs = options.wsMaxBufferStallMs != null ? options.wsMaxBufferStallMs : 10000;
  this._overLimitSince = 0;
  this._pendingSends = 0;
  this._finalCallback = null;

  let self = this;

  this._onMessage = this._onMessage.bind(this);
  this._onClose = function() {
    self._stopClient();
  };
  client.on('message', this._onMessage);
  client.on('close', this._onClose);

  // An 'error' without a listener would crash the process
  this.on('error', function(err) {
    if (err.code === 'ERR_STREAM_WRITE_AFTER_END' || err.code === 'ERR_STREAM_DESTROYED') return;
    console.warn('[racer-highway] WebSocket client message stream error', err);
  });

  // Safety net: if Node destroys the stream anyway (e.g. ShareDB writes after
  // Agent.close() ended it), make sure ShareDB still sees 'end'.
  this.on('close', function() {
    self._stopClient();
    if (!self.readableEnded) self.emit('end');
  });
}
util.inherits(ClientStream, Duplex);

ClientStream.prototype._read = function() {};

ClientStream.prototype._onMessage = function(data) {
  let message;
  try {
    message = JSON.parse(data.toString());
  } catch (err) {
    console.warn('[racer-highway] Invalid JSON from client, terminating connection:', describeClient(this.client), err.message);
    return this._stopClient();
  }
  this.push(message);
};

ClientStream.prototype._write = function(chunk, encoding, callback) {
  let self = this;
  let client = this.client;

  // The connection is gone: drop the message. ShareDB stops sending once it
  // sees 'end'.
  if (!client) return callback();
  if (client.readyState !== WebSocket.OPEN) {
    this._stopClient();
    return callback();
  }

  if (this._isStalled(client)) {
    let currentMB = (client.bufferedAmount / 1024 / 1024).toFixed(2);
    let limitMB = (this._bufferLimit / 1024 / 1024).toFixed(2);
    console.warn(`[racer-highway] OOM Protection: Killing client. Buffer: ${currentMB} MB > ${limitMB} MB for ${this._bufferStallMs} ms. ${describeClient(client)}`);
    this._stopClient();
    return callback();
  }

  let data;
  try {
    data = JSON.stringify(chunk);
  } catch (err) {
    console.error('[racer-highway] Could not serialize message:', err);
    this._stopClient();
    return callback();
  }

  // Don't wait for the send to flush. Unsent data then sits in the socket
  // buffer, where bufferedAmount measures it for _isStalled().
  this._pendingSends++;
  client.send(data, function(err) {
    self._pendingSends--;
    // Errors after _stopClient() are expected: terminate() fails pending sends
    if (err && self.client) {
      console.error('[racer-highway] send:', err);
      self._stopClient();
    }
    self._flushFinal();
  });
  callback();
};

// True once the send buffer has stayed above wsMaxBufferMB for
// wsMaxBufferStallMs. A burst that drains in time is fine; a client that can't
// keep up is not.
ClientStream.prototype._isStalled = function(client) {
  if (client.bufferedAmount <= this._bufferLimit) {
    this._overLimitSince = 0;
    return false;
  }
  let now = Date.now();
  if (!this._overLimitSince) this._overLimitSince = now;
  return now - this._overLimitSince >= this._bufferStallMs;
};

// Agent.close() ends the writable side. Let queued messages (e.g. the error
// that closed the agent) reach the socket before terminating.
ClientStream.prototype._final = function(callback) {
  this._finalCallback = callback;
  this._flushFinal();
};

ClientStream.prototype._flushFinal = function() {
  let callback = this._finalCallback;
  if (!callback || (this.client && this._pendingSends > 0)) return;
  this._finalCallback = null;
  this._stopClient();
  callback();
};

ClientStream.prototype._stopClient = function() {
  let client = this.client;
  if (!client) return;
  this.client = null;

  client.removeListener('message', this._onMessage);
  client.removeListener('close', this._onClose);
  try {
    // terminate() drops the TCP connection right away and frees the buffers
    client.terminate();
  } catch (err) {
    console.error('[racer-highway] Error while terminating WebSocket in _stopClient:', err);
  }

  // End the readable side: ShareDB cleans up the agent on 'end'
  this.push(null);
  this._flushFinal();
};
