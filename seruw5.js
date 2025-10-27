server.on('upgrade', (req, socket, head) => {
  if (parseUrl(req.url).pathname !== '/ws') return socket.destroy();
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
});
