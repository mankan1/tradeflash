//const { broadcastOptFlowBatch } = require("./publish");
import { broadcastOptFlowBatch } from "./publish.js";

const seen = new Set();
let buffer = [];
let timer = null;

export function enqueueFlow(wss, items) {
  if (!Array.isArray(items) || !items.length) return;
  for (const it of items) {
    if (!it || !it.id || seen.has(it.id)) continue;
    seen.add(it.id);
    buffer.push(it);
  }
  if (!timer) {
    timer = setTimeout(() => {
      const out = buffer.sort((a, b) => b.ts - a.ts).slice(0, 1000);
      buffer = [];
      timer = null;
      if (out.length) broadcastOptFlowBatch(wss, out);
      if (seen.size > 20000) seen.clear();
    }, 250);
  }
}

//module.exports = { enqueueFlow };
