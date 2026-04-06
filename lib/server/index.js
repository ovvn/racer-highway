var WebSocket = require('ws');
var WebSocketServer = WebSocket.Server;
var BrowserChannelServer = require('browserchannel').server;
var crypto = require('crypto');
var extend = require('extend');
var createBrowserChannelStream = require('./createBrowserChannelStream');
var createWebSocketStream = require('./createWebSocketStream');

var bundle = require('./bundle');

var defaultServerOptions = {
  session: null,
  base: '/channel',
  noPing: false,
  pingInterval: 5000,
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

  wss.on('connection', function (client) {
    client.isAlive = true;

    client.on('pong', function() {
      client.isAlive = true;
    });

    client.id = crypto.randomBytes(16).toString('hex');
    let stream = createWebSocketStream(client, serverOptions);

    client.on('error', function(error) {
      // EPIPE is expected when terminate() races with a close frame — not actionable
      if (error.code === 'EPIPE' || error.code === 'ECONNRESET') return;
      console.error('WebSocket client error:', error);
      // Handle client-specific errors here
      // Optionally, you can close the connection
      if (stream && stream._stopClient) {
        stream._stopClient();
      } else {
        client.terminate();
      }
    });

    // Some proxy drop out long connections
    // so do ping periodically to prevent this
    // interval = 10s by default
    if (!serverOptions.noPing) {
      client.timer = setInterval(function() {
        // Если с момента прошлого пинга клиент так и не ответил pong
        if (client.isAlive === false) {
          console.warn('[racer-highway] Client heartbeat failure. Terminating connection to save RAM.');

          // Очищаем таймер перед уничтожением
          clearInterval(client.timer);

          // terminate() немедленно рвет TCP-соединение и очищает буферы
          return client.terminate();
        }

        // Если клиент жив, помечаем его как "неактивного" и шлем новый пинг
        if (client.readyState === WebSocket.OPEN) {
          client.isAlive = false;
          client.ping();
        } else {
          clearInterval(client.timer);
        }
      }, serverOptions.pingInterval);
    }

    var rejected = false;
    var rejectReason;

    function reject(reason) {
      rejected = true;
      if (reason) rejectReason = reason;
    }

    if (client.upgradeReq.session) client.connectSession = client.upgradeReq.session;

    backend.emit('client', client, reject);
    if (rejected) {
      // Tell the client to stop trying to connect
      client.close(1001, rejectReason);
      clearInterval(client.timer)
      return;
    }
    doneInitialization(backend, stream, client.upgradeReq);

  });

  wss.on('error', function (error) {
    console.error('WebSocket Server error:', error);
  });

  function upgrade(req, socket, upgradeHead){
    //copy upgradeHead to avoid retention of large slab buffers used in node core
    const head = Buffer.from(upgradeHead);
    upgradeHead.copy(head);

    if (serverOptions.session) {
      // https://github.com/expressjs/session/pull/57
      if (!req.originalUrl) req.originalUrl = req.url;
      serverOptions.session(req, {}, next);
    } else {
      next();
    }

    function next() {
      wss.handleUpgrade(req, socket, head, function(client) {
        wss.emit('connection'+req.url, client);
        wss.emit('connection', client);
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
  let cleaned = false;
  function cleanup() {
    if (cleaned) return;
    cleaned = true;
    clearTimeout(safetyTimer);
    backend.removeListener('connect', connectHandler);
  }

  function connectHandler(data) {
    let agent = data.agent;
    // Only handle our own agent by checking stream identity
    if (agent && agent.stream === stream) {
      if (request.session) agent.connectSession = request.session;
      backend.emit('share agent', agent, stream);
      backend.removeListener('connect', connectHandler); // clean up immediately
    }

  }
  backend.on('connect', connectHandler);
  backend.listen(stream, request);

  // Fallback: clean up after 30s if nothing fired
  const safetyTimer = setTimeout(cleanup, 30000);

  // Clean up listeners on disconnect
  stream.once('end', cleanup);
  stream.once('close', cleanup);
  stream.once('error', cleanup);
  stream.once('finish', cleanup);
}
