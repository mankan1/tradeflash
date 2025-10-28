export function broadcastOptFlowBatch(wss, items) {
  if (!wss || !items?.length) return;
  const msg = JSON.stringify({ type: "opt_flow_batch", items });
  for (const c of wss.clients) if (c.readyState === 1) c.send(msg);
}

//module.exports = { broadcastOptFlowBatch };
