module.exports = describeClient;

/**
 * One-line summary of a WebSocket client for log messages
 * @param {WebSocket} client
 * @return {String}
 */
function describeClient(client) {
  if (!client) return 'client=unknown';

  let parts = [
    'id=' + (client.id || 'unknown'),
    'ip=' + (client.remoteAddress || 'unknown')
  ];

  let session = client.connectSession;
  let userId = session && (session.userId || (Array.isArray(session.userIds) && session.userIds.join(',')));
  if (userId) parts.push('user=' + userId);

  if (client.connectedAt) {
    parts.push('age=' + Math.round((Date.now() - client.connectedAt) / 1000) + 's');
  }
  parts.push('buffered=' + Math.round((client.bufferedAmount || 0) / 1024) + 'KB');

  return parts.join(' ');
}
