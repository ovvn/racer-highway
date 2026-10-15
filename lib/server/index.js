let { WebSocket, WebSocketServer } = require('ws');
let BrowserChannelServer = require('browserchannel').server;
let crypto = require('crypto');
let extend = require('extend');
let createBrowserChannelStream = require('./createBrowserChannelStream');
let createWebSocketStream = require('./createWebSocketStream');
let describeClient = require('./describeClient');

let bundle = require('./bundle');

var defaultServerOptions = {
  session: null,
  base: '/channel',
  noPing: false,
  // A client is terminated once maxMissedPongs pings in a row go unanswered,
  // i.e. 20-30s after it dies. Tolerating a miss keeps an event-loop stall or
  // a slow drain of a large message from killing live clients.
  pingInterval: 10000,
  maxMissedPongs: 2,
  // A client whose send buffer stays above wsMaxBufferMB for
  // wsMaxBufferStallMs is terminated
  wsMaxBufferMB: 5,
  wsMaxBufferStallMs: 10000,
  // Drop upgrade requests whose session lookup takes longer than this
  upgradeTimeout: 30000,
  perMessageDeflate: false
};

module.exports = function(backend, serverOptions, clientOptions) {

  serverOptions = serverOptions || {};
  serverOptions = extend({}, defaultServerOptions, serverOptions);

  // ws-module specific options
  serverOptions.path = serverOptions.base;
  serverOptions.noServer = true;

  // add the client side script to the Browserify bundle
  backend.on('bundle', bundle(clientOptions));

  var middleware = BrowserChannelServer(serverOptions, function(client, connectRequest) {

    if (serverOptions.session) {
      // https://github.com/expressjs/session/pull/57
      if (!connectRequest.originalUrl) connectRequest.originalUrl = connectRequest.url;
      serverOptions.session(connectRequest, {}, startBrowserChannel);
    } else {
      startBrowserChannel();
    }

    function startBrowserChannel() {
      var rejected = false;
      var rejectReason;
      function reject(reason) {
        rejected = true;
        if (reason) rejectReason = reason;
      }

      if (connectRequest.session) client.connectSession = connectRequest.session;

      backend.emit('client', client, reject);
      if (rejected) {
        // Tell the client to stop trying to connect
        client.stop(function() {
          client.close(rejectReason);
        });
        return;
      }
      let stream = createBrowserChannelStream(client);
      doneInitialization(backend, stream, connectRequest);
    }
  });

  var wss = new WebSocketServer(serverOptions);

  wss.on('connection', function (client, req) {
    client.id = crypto.randomBytes(16).toString('hex');
    client.connectedAt = Date.now();
    client.remoteAddress = getRemoteAddress(req);
    client.missedPongs = 0;

    client.on('pong', function() {
      client.missedPongs = 0;
    });

    let stream;

    client.on('error', function(error) {
      // EPIPE is expected when terminate() races with a close frame — not actionable
      if (error.code === 'EPIPE' || error.code === 'ECONNRESET') return;
      console.error('[racer-highway] WebSocket client error:', describeClient(client), error);
      // ws emits 'close' after an error, which also stops the stream
      if (stream) {
        stream._stopClient();
      } else {
        client.terminate();
      }
    });

    var rejected = false;
    var rejectReason;

    function reject(reason) {
      rejected = true;
      if (reason) rejectReason = reason;
    }

    if (req.session) client.connectSession = req.session;

    backend.emit('client', client, reject);
    if (rejected) {
      // Tell the client to stop trying to connect
      client.close(1001, toCloseReason(rejectReason));
      return;
    }

    stream = createWebSocketStream(client, serverOptions);
    doneInitialization(backend, stream, req);
  });

  wss.on('error', function (error) {
    console.error('WebSocket Server error:', error);
  });

  // Some proxies drop idle connections, so ping periodically. The pings also
  // find dead clients, which would otherwise hold their agents and buffers.
  if (!serverOptions.noPing) {
    let heartbeat = setInterval(function() {
      wss.clients.forEach(function(client) {
        if (client.readyState !== WebSocket.OPEN) return;

        if (client.missedPongs >= serverOptions.maxMissedPongs) {
          console.warn('[racer-highway] Client heartbeat failure. Terminating connection:', describeClient(client));
          return client.terminate();
        }

        client.missedPongs++;
        client.ping();
      });
    }, serverOptions.pingInterval);

    wss.on('close', function() {
      clearInterval(heartbeat);
    });
  }

  function upgrade(req, socket, upgradeHead) {
    const head = Buffer.from(upgradeHead);

    // Guard: session middleware (e.g. connect-mongo) can invoke the callback
    // more than once. ws v8 throws if handleUpgrade() is called twice on the
    // same socket, so we gate it with a flag on the socket itself.
    let handled = false;
    let timer;

    if (!serverOptions.session) return next();

    // Node removes its own 'error' listener from the socket before emitting
    // 'upgrade'. Until ws takes over, a connection reset during the session
    // lookup would be an unhandled 'error' and crash the process.
    socket.on('error', onSocketError);
    timer = setTimeout(function() {
      handled = true;
      socket.destroy();
    }, serverOptions.upgradeTimeout);

    if (!req.originalUrl) req.originalUrl = req.url;
    serverOptions.session(req, {}, next);

    function onSocketError() {
      socket.destroy();
    }

    function next(err) {
      if (handled) return;
      handled = true;
      clearTimeout(timer);
      if (err) console.warn('[racer-highway] Session lookup failed during upgrade:', err.message || err);
      if (socket.destroyed) return;

      // ws adds its own 'error' listener in handleUpgrade()
      socket.removeListener('error', onSocketError);
      wss.handleUpgrade(req, socket, head, function(client) {
        // ws v8: pass req as second arg to 'connection' — no more double-emit
        wss.emit('connection', client, req);
      });
    }
  }

  if (serverOptions.session) {
    backend.use('connect',  function(shareRequest, next){
      var req = shareRequest.req;
      var agent = shareRequest.agent;

      if (!agent.connectSession && req && req.session) {
        agent.connectSession = req.session;
      }

      // TODO check if we really need the code
      // for now it doesn't work anymore
      // because sharedb 'connect' hook can work
      // only synchronously

      // serverOptions.session(req, {}, function(){
      //   agent.connectSession = req.session;
      //   next();
      // });

      next()
    });
  }

  return {upgrade: upgrade, middleware: middleware, wss: wss};
};

function doneInitialization(backend, stream, request) {
  let agent = backend.listen(stream, request);
  if (request.session) agent.connectSession = request.session;
  backend.emit('share agent', agent, stream);
}

function getRemoteAddress(req) {
  let forwarded = req.headers['x-forwarded-for'];
  if (forwarded) return forwarded.split(',')[0].trim();
  return req.socket && req.socket.remoteAddress;
}

// ws throws if the close reason is longer than 123 bytes
function toCloseReason(reason) {
  if (reason == null) return undefined;
  let text = typeof reason === 'string' ? reason : (reason.message || String(reason));
  return Buffer.byteLength(text) <= 123 ? text : undefined;
}
