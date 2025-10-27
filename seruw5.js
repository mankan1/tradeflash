import "dotenv/config";
import express from "express";
import cors from "cors";

import { WebSocketServer } from 'ws';
import { WebSocket } from 'ws';

import * as ES from "eventsource"; const EventSource = ES.default ?? ES;
import axios from "axios";
import { DateTime } from "luxon";
import http from 'http';
import { createServer } from 'http';
// near top with other imports
import { POLY, startPolygonWatch } from "./polygon.js";

// --- Logging bootstrap ---
import morgan from "morgan";
import { v4 as uuidv4 } from "uuid";
import { AsyncLocalStorage } from "node:async_hooks";
// ---- Alpaca equity "flow" (polling latest trades) ----
import { ALP } from "./polygon.js"; // path as per your project


attachAxiosLogging(axios, "tradier");      // your default axios (Tradier usage)
//attachAxiosLogging(ALPACA, "alpaca");      // your Alpaca axios instance
//attachAxiosLogging(POLY, "polygon");       // your Polygon axios instance

export const reqStore = new AsyncLocalStorage();

const app = express();
// app.use(cors());
// app.use(cors({
//   origin: '*', // Allow all origins, or specify: 'https://tradeflashcli.vercel.app'
//   credentials: true
// }));

// allow your local dev + your deployed web app(s)
// const ALLOW_ORIGINS = [
//   "http://localhost:8081",          // your local expo web/serve port
//   "http://localhost:19006",         // expo default web dev port (if used)
//   "https://tradeflashcli.vercel.app", // your Vercel client
//   // add any others you actually use
// ];

// app.use(
//   cors({
//     origin: function (origin, cb) {
//       // allow no-origin (curl, health checks) and exact matches
//       if (!origin || ALLOW_ORIGINS.includes(origin)) return cb(null, true);
//       return cb(new Error("CORS: origin not allowed: " + origin), false);
//     },
//     methods: ["GET", "POST", "OPTIONS"],
//     allowedHeaders: ["Content-Type", "x-request-id", "x-requested-with"],
//     credentials: false, // keep false unless you really need cookies/auth
//   })
// );

// // Good practice: handle preflight explicitly
// app.options("*", cors());

const ORIGINS = [
  "https://tradeflash.pro",
  "https://tradeflashcli.vercel.app",
  "http://localhost:3000",
  "http://localhost:5173",
  "http://localhost:8080",
];

app.use((req, res, next) => {
  // help caches pick correct variant per Origin
  res.header("Vary", "Origin");
  next();
});

app.use(
  cors({
    origin(origin, cb) {
      if (!origin) return cb(null, true);                 // curl / server-to-server
      if (ORIGINS.includes(origin)) return cb(null, true);
      return cb(new Error(`CORS: origin not allowed: ${origin}`));
    },
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "x-request-id"],
    credentials: false, // set to true only if you really need cookies/auth
    maxAge: 86400,
  })
);

// reply to preflight
app.options("*", cors());

// Important: Add CORS and headers
// app.use((req, res, next) => {
//   res.header('Access-Control-Allow-Origin', '*');
//   res.header('Access-Control-Allow-Headers', '*');
//   next();
// });

app.use(express.json({ limit: "64kb" }));

// Attach a request-id and put it into AsyncLocalStorage
app.use((req, res, next) => {
  const rid = req.headers["x-request-id"]?.toString() || uuidv4();
  req.id = rid;
  // expose provider (if any) for logging
  const provider = (req.query?.provider || "").toString().toLowerCase() || "n/a";

  reqStore.run({ rid, provider, started: Date.now() }, () => {
    res.setHeader("x-request-id", rid);
    next();
  });
});

// morgan token for req.id and provider
morgan.token("rid", (req) => req.id);
morgan.token("provider", (req) => (req.query?.provider || "n/a").toString());

// Example format: [rid] METHOD URL status len - response-time ms (provider)
app.use(
  morgan(
    '[:rid] :method :url :status :res[content-length] - :response-time ms (provider=:provider)'
  )
);

//const { TRADIER_BASE="", TRADIER_TOKEN="", PORT=RENDER_PORT, WS_PORT=8081 } = process.env;
const { TRADIER_BASE="", TRADIER_TOKEN="", PORT=RENDER_PORT, WS_PORT=RENDER_PORT } = process.env;
if (!TRADIER_BASE || !TRADIER_TOKEN) throw new Error("Missing TRADIER_* envs");

//const app = express();
//app.use(cors());


const server = http.createServer(app);

// const WebSocket = require('ws')
// const PORT1 = process.env.PORT || 3000;
const PORT1 = process.env.PORT || 10000

//const wss = new WebSocket.Server({ server })

// Create WebSocket server with proper config

// const wss = new WebSocket.Server({ 
// const wss = new WebSocketServer({ 
//   server,
//   perMessageDeflate: false, // Important for Render
//   clientTracking: true
// });

// 2) WS attached to SAME server + explicit path
const wss = new WebSocketServer({
  server,
  path: "/ws",              // <— explicit path prevents ambiguity
  clientTracking: true,
  perMessageDeflate: false, // friendlier behind proxies
});

// WebSocket connections
wss.on('connection', (ws) => {
  console.log('WebSocket client connected')

  ws.on('message', (message) => {
    console.log('Received:', message.toString())
    ws.send(`Hello over WebSocket!`)
  })
})

// 4) keep-alive (important behind proxies)
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { try { ws.terminate(); } catch {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 30000);

//const wss = new WebSocketServer({ server, path: "/ws" }); //({ port: Number(WS_PORT) });

const broadcast = (msg) => {
  const s = JSON.stringify(msg);
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
};

// const H_JSON = { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: "application/json" };
// const H_SSE  = { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: "text/event-stream" };
const isSandbox = TRADIER_BASE.includes("sandbox");
const TS_INTERVAL = isSandbox ? "1min" : "tick";
const errToJson = (e) => (e?.response ? { status: e.response.status, data: e.response.data } : { message: String(e) });

// ---- dynamic credentials (in-memory) ----
const CREDENTIALS = {
  tradier: { token: process.env.TRADIER_TOKEN || "" },
  alpaca:  { key: process.env.APCA_API_KEY_ID || "", secret: process.env.APCA_API_SECRET_KEY || "" },
};

// dynamic headers for Tradier (read current token)
function H_JSON() {
  return { Authorization: `Bearer ${CREDENTIALS.tradier.token}`, Accept: "application/json" };
}
// function H_SSE() {
//   return { Authorization: `Bearer ${CREDENTIALS.tradier.token}`, Accept: "text/event-stream" };
// }
function H_SSE() {
  return {
    Authorization: `Bearer ${CREDENTIALS.tradier.token}`,
    Accept: "text/event-stream",
    "Cache-Control": "no-cache",
    "Accept-Encoding": "identity",
    Connection: "keep-alive"
  };
}
function parseDay(req) {
  const d = String(req.query.day || "").trim();
  if (d && /^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
  const nowNY = DateTime.now().setZone("America/New_York");
  if (nowNY.weekday === 6) return nowNY.minus({ days: 1 }).toFormat("yyyy-LL-dd"); // Sat->Fri
  if (nowNY.weekday === 7) return nowNY.minus({ days: 2 }).toFormat("yyyy-LL-dd"); // Sun->Fri
  return nowNY.toFormat("yyyy-LL-dd");
}

const alpLastBySym = new Map(); // symbol -> last trade ID (or ts/price) we’ve seen

/* ------------------------ side inference + books (existing) ------------------------ */
const midBySym = new Map();        // symbol -> { bid, ask, mid }
const lastTradeBySym = new Map();  // symbol -> last trade price we broadcast
function updateMidFromQuote(q) {
  const bid = Number(q.bid ?? NaN);
  const ask = Number(q.ask ?? NaN);
  if (!Number.isFinite(bid) && !Number.isFinite(ask)) return;
  const prev = midBySym.get(q.symbol) || {};
  const newBid = Number.isFinite(bid) ? bid : prev.bid;
  const newAsk = Number.isFinite(ask) ? ask : prev.ask;
  const mid = (Number.isFinite(newBid) && Number.isFinite(newAsk)) ? (newBid + newAsk) / 2 : prev.mid;
  if (Number.isFinite(newBid) || Number.isFinite(newAsk) || Number.isFinite(mid)) {
    midBySym.set(q.symbol, { bid: newBid, ask: newAsk, mid });
  }
}
const epsFor = ({ mid, bid, ask }) => {
  if (Number.isFinite(bid) && Number.isFinite(ask)) return Math.max((ask - bid) * 0.15, 0.01);
  if (Number.isFinite(mid)) return Math.max(0.001 * mid, 0.01);
  return 0.01;
};
function inferSideServer(sym, price) {
  const book = midBySym.get(sym);
  if (book) {
    const { bid, ask, mid } = book; const eps = epsFor(book);
    if (Number.isFinite(ask) && price >= (ask - eps)) return { side: "BOT", side_src: "mid" };
    if (Number.isFinite(bid) && price <= (bid + eps)) return { side: "SLD", side_src: "mid" };
    if (Number.isFinite(mid)) {
      if (price > mid + eps) return { side: "BOT", side_src: "mid" };
      if (price < mid - eps) return { side: "SLD", side_src: "mid" };
    }
  }
  const prev = lastTradeBySym.get(sym);
  if (typeof prev === "number") {
    if (price > prev) return { side: "BOT", side_src: "tick" };
    if (price < prev) return { side: "SLD", side_src: "tick" };
  }
  return { side: "—", side_src: "none" };
}

/* ------------------------ option state + action heuristic (existing) ------------------------ */
const oiByOcc = new Map();   // OCC -> open_interest
const volByOcc = new Map();  // OCC -> last seen cumulative volume
const TH_CLOSE_BY_DVOL = 0.8; // ≥80% of OI already traded -> close-lean
function aggressorFrom(side, at) {
  if (side === "BOT" || at === "ask") return "buy";
  if (side === "SLD" || at === "bid") return "sell";
  return "unknown";
}
function classifyOpenClose({ qty, oi, priorVol, side, at }) {
  const oi0 = Number(oi) || 0;
  const prior = Number(priorVol) || 0;
  const aggr = aggressorFrom(side, at);
  if (qty > (oi0 + prior)) {
    if (aggr === "buy") return { action: "BTO", action_conf: "high" };
    if (aggr === "sell") return { action: "STO", action_conf: "high" };
    return { action: "OPEN?", action_conf: "medium" };
  }
  const dVolShare = prior / Math.max(1, oi0);
  if (dVolShare >= TH_CLOSE_BY_DVOL) {
    if (aggr === "buy")  return { action: "BTC", action_conf: "medium" };
    if (aggr === "sell") return { action: "STC", action_conf: "medium" };
    return { action: "CLOSE?", action_conf: "low" };
  }
  return { action: "—", action_conf: "low" };
}

async function pollAlpacaLatestTrades(symbols = []) {
  if (!symbols.length) return;
  try {
    const { data } = await ALP.get("/v2/stocks/trades/latest", {
      params: { symbols: symbols.join(",") },
      validateStatus: () => true,
    });
    const snaps = data?.trades || data?.latestTrades || data; // handle shapes

    for (const sym of symbols) {
      const row = snaps?.[sym];
      if (!row) continue;

      // Normalize
      const price = Number(row.p ?? row.price ?? 0);
      const size  = Number(row.s ?? row.size ?? 0);
      const ts    = Number(row.t ?? row.timestamp ?? Date.now());
      const id    = String(row.i ?? row.id ?? `${sym}_${ts}_${price}`);

      if (!(price > 0 && size > 0)) continue;

      // de-dupe
      const prevId = alpLastBySym.get(sym);
      if (prevId === id) continue;
      alpLastBySym.set(sym, id);

      // update book approximation from a quick quote snapshot (optional)
      // you can skip this if you’re happy with tick-based side only
      let at = "between";
      let side_src = "tick";
      const { side } = inferSideServer(sym, price); // your existing mid/tick logic chooses a side, src "mid" if book known

      // Broadcast in our unified shape and tag provider
      broadcast({
        type: "equity_ts",
        symbol: sym,
        provider: "alpaca",
        data: { time: ts, price, size, side, side_src, at }
      });
    }
  } catch (e) {
    console.error("pollAlpacaLatestTrades error:", errToJson(e));
  }
}

async function polyTopMovers() {
  // /v2/snapshot/locale/us/markets/stocks/gainers | losers
  const [g, l] = await Promise.all([
    POLY.get("/v2/snapshot/locale/us/markets/stocks/gainers"),
    POLY.get("/v2/snapshot/locale/us/markets/stocks/losers"),
  ]);
  // Each row has: ticker, day, lastTrade, min, prevDay, etc.
  return {
    gainers: Array.isArray(g.data?.tickers) ? g.data.tickers : [],
    losers:  Array.isArray(l.data?.tickers) ? l.data.tickers : [],
  };
}
async function polyMostActives({ by="volume", top=30 } = {}) {
  // /v2/snapshot/locale/us/markets/stocks/tickers
  const r = await POLY.get("/v2/snapshot/locale/us/markets/stocks/tickers");
  const rows = Array.isArray(r.data?.tickers) ? r.data.tickers : [];
  // derive fields
  const norm = rows.map(t => {
    const vol = Number(t.day?.v ?? t.day?.volume ?? 0);
    const tradeCount = Number(t.day?.n ?? t.day?.transactions ?? 0);
    const last = Number(t.lastTrade?.p ?? 0);
    return {
      symbol: t.ticker,
      volume: vol,
      trade_count: tradeCount,
      last,
      change_percent: Number(t.todaysChangePerc ?? 0) / 100, // polygon gives % already; normalize to 0..1
    };
  });
  const key = by === "trade_count" ? "trade_count" : "volume";
  return norm.sort((a,b)=> (b[key]||0)-(a[key]||0)).slice(0, top);
}
async function backfillEquityTS(symbol, day, minutes = 5) {
  try {
    const startISO = nyDayStartISO(day);
    const endDt = new Date(new Date(startISO).getTime() + minutes*60*1000);
    const endISO = endDt.toISOString().slice(0,19).replace('T',' ');

    const frames = await fetchTimesalesChunked({
      symbol, day, interval: "1min", startISO, endISO
    });

    for (const bar of frames) {
      const price = Number(bar.close ?? bar.price ?? bar.last ?? 0);
      const size  = Number(bar.volume ?? bar.size ?? 1);
      if (!(price > 0 && size > 0)) continue;

      const { side, side_src } = inferSideServer(symbol, price);
      lastTradeBySym.set(symbol, price);

      const book = midBySym.get(symbol) || {};
      const eps  = epsFor(book);
      let at = "between";
      if (book.ask && price >= book.ask - eps) at = "ask";
      else if (book.bid && price <= book.bid + eps) at = "bid";
      else if (book.mid && Math.abs(price - book.mid) <= eps) at = "mid";

      broadcast({
        type: "equity_ts",
        symbol,
        data: { ...bar, price, size, side, side_src, at, book }
      });
    }
  } catch (e) {
    console.error("backfillEquityTS error:", errToJson(e));
  }
}

/* ------------------------ equity backfill (existing) ------------------------ */
// async function backfillEquityTS(symbol, day, minutes = 5) {
//   try {
//     const start = DateTime.fromISO(`${day}T09:30:00`, { zone: "America/New_York" });
//     const end = start.plus({ minutes });
//     const params = {
//       symbol, interval: TS_INTERVAL,
//       start: start.toFormat("yyyy-LL-dd HH:mm:ss"),
//       end: end.toFormat("yyyy-LL-dd HH:mm:ss"),
//     };
//     const { data } = await axios.get(`${TRADIER_BASE}/markets/timesales`, { headers: H_JSON(), params });
//     (data?.series?.data ?? []).forEach(tick => {
//       const price = Number(tick.price ?? tick.last ?? tick.close ?? 0);
//       const { side, side_src } = inferSideServer(symbol, price);
//       lastTradeBySym.set(symbol, price);

//       const book = midBySym.get(symbol) || {};
//       const eps = epsFor(book);
//       let at = "between";
//       if (book.ask && price >= book.ask - eps) at = "ask";
//       else if (book.bid && price <= book.bid + eps) at = "bid";
//       else if (book.mid && Math.abs(price - book.mid) <= eps) at = "mid";

//       broadcast({ type: "equity_ts", symbol, data: { ...tick, side, side_src, at, book } });
//     });
//   } catch (e) {
//     console.error("backfillEquityTS error:", errToJson(e));
//   }
// }

/* ------------------------ options helpers (existing) ------------------------ */
async function getExpirations(symbol) {
  const { data } = await axios.get(`${TRADIER_BASE}/markets/options/expirations`, { headers: H_JSON(), params: { symbol } });
  return data?.expirations?.date ?? [];
}
async function getUnderlyingLast(symbol) {
  const { data } = await axios.get(`${TRADIER_BASE}/markets/quotes`, { headers: H_JSON(), params: { symbols: symbol } });
  const q = data?.quotes?.quote;
  return Array.isArray(q) ? (+q[0]?.last || +q[0]?.close || 0) : (+q?.last || +q?.close || 0);
}
async function buildOptionsWatchList(symbol, { expiries=[], moneyness=0.25, limit=150 }={}) {
  const out = [];
  const und = await getUnderlyingLast(symbol);
  let exps = expiries.length ? expiries : (await getExpirations(symbol)).slice(0,1);
  for (const exp of exps) {
    const { data } = await axios.get(`${TRADIER_BASE}/markets/options/chains`, { headers: H_JSON(), params: { symbol, expiration: exp } });
    const arr = data?.options?.option || [];
    for (const o of arr) {
      oiByOcc.set(o.symbol, Number(o.open_interest || 0));
      if (Number.isFinite(Number(o.volume))) volByOcc.set(o.symbol, Number(o.volume));
      const within = und ? Math.abs(o.strike - und) / und <= moneyness : true;
      if (within) out.push(o.symbol);
      if (out.length >= limit) break;
    }
    if (out.length >= limit) break;
  }
  return out;
}
/* ------------------------ streaming quotes (robust) ------------------------ */
// let esQuotes: EventSource | null = null;

// function startQuoteStream(symbolsOrOCC: string[] = []) {
//   try {
//     if (esQuotes) esQuotes.close();
//     const uniq = [...new Set(symbolsOrOCC)].filter(Boolean);
//     if (uniq.length === 0) return;

//     const url = `${TRADIER_BASE}/markets/events?symbols=${encodeURIComponent(
//       uniq.join(",")
//     )}&sessionid=${Date.now()}`;
//     const es = new EventSource(url, { headers: H_SSE() });
//     esQuotes = es;

//     // --- DEBUG counters ---
//     let qCnt = 0,
//       tCnt = 0,
//       occQuoteCnt = 0,
//       occTradeCnt = 0,
//       optionTsCnt = 0;

//     es.onmessage = (e: MessageEvent) => {
//       try {
//         const msg = JSON.parse(e.data);

//         // ---- increment counters sanely (no ++(ternary)) ----
//         if (msg?.type === "quote") qCnt++;
//         else if (msg?.type === "trade") tCnt++;
//         else qCnt++; // bucket unknowns with quotes

//         // sample log every 200th message of that type
//         const cur =
//           msg?.type === "quote" ? qCnt : msg?.type === "trade" ? tCnt : qCnt;
//         if (msg?.type && cur % 200 === 1) {
//           console.log(
//             `[SSE ${msg.type}] sample`,
//             msg?.data?.symbol || msg?.symbol,
//             { qCnt, tCnt }
//           );
//         }

//         // ================== TRADE path ==================
//         if (msg?.type === "trade" && msg?.data) {
//           const tr: any = msg.data;
//           const sym = tr.symbol;
//           const isOCC = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(sym);
//           if (isOCC) occTradeCnt++;

//           const price = Number(tr.price ?? tr.last ?? tr.p ?? 0);
//           const qty = Number(tr.size ?? tr.s ?? tr.quantity ?? tr.last_size ?? 0);

//           // keep NBBO if available on this frame
//           if (tr.bid != null || tr.ask != null) updateMidFromQuote(tr);

//           if (isOCC && price > 0 && qty > 0) {
//             const { side, side_src } = inferSideServer(sym, price);
//             const book = midBySym.get(sym) || {};
//             const eps = epsFor(book);
//             let at: "bid" | "ask" | "mid" | "between" = "between";
//             if (book.ask && price >= book.ask - eps) at = "ask";
//             else if (book.bid && price <= book.bid + eps) at = "bid";
//             else if (book.mid && Math.abs(price - book.mid) <= eps) at = "mid";

//             // update vol trackers for open/close heuristic
//             const seenVol = Number(tr.volume ?? NaN);
//             let priorVol = volByOcc.has(sym) ? volByOcc.get(sym) : 0;
//             if (Number.isFinite(seenVol)) priorVol = Math.max(0, seenVol - qty);
//             if (Number.isFinite(seenVol)) volByOcc.set(sym, seenVol);
//             else volByOcc.set(sym, (volByOcc.get(sym) || 0) + qty);
//             const oi = oiByOcc.get(sym) ?? null;

//             const { action, action_conf } = classifyOpenClose({
//               qty,
//               oi,
//               priorVol,
//               side,
//               at,
//             });

//             optionTsCnt++;
//             if (optionTsCnt % 100 === 1) {
//               console.log("[SRV option_ts TRADE]", sym, {
//                 qty,
//                 price,
//                 side,
//                 at,
//                 action,
//                 priorVol,
//                 oi,
//               });
//             }

//             broadcast({
//               type: "option_ts",
//               symbol: sym,
//               provider: "tradier",
//               data: {
//                 id: `ots_${sym}_${tr.trade_time || Date.now()}`,
//                 ts: Date.now(),
//                 option: {
//                   expiry: "",
//                   strike: 0,
//                   right: /C\d{8}$/i.test(sym) ? "C" : "P",
//                 },
//                 qty,
//                 price,
//                 side,
//                 side_src,
//                 oi,
//                 priorVol,
//                 book,
//                 at,
//                 action,
//                 action_conf,
//               },
//             });
//             return;
//           }

//           // non-OCC trades: fall through (equity prints handled via quotes path too)
//         }

//         // ================== QUOTE path ==================
//         if (msg?.type === "quote" && msg?.data) {
//           const q: any = msg.data;

//           // always keep NBBO cache fresh
//           if (q?.bid != null || q?.ask != null) updateMidFromQuote(q);

//           const isOCC = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(q.symbol);
//           if (isOCC) occQuoteCnt++;

//           // “trade-like” fields sometimes ride on quote frames
//           const price = Number(q.last ?? q.price ?? q.p ?? 0);
//           const qty = Number(
//             q.size ?? q.trade_size ?? q.last_size ?? q.quantity ?? 0
//           );

//           if (price > 0 && qty > 0) {
//             // Broadcast quotes (equity prints + context) with a side on the root
//             const rootSym = isOCC
//               ? q.symbol.replace(/\d{6}[CP]\d{8}$/, "")
//               : q.symbol;
//             const { side, side_src } = inferSideServer(rootSym, price);
//             broadcast({ type: "quotes", data: q, side, side_src, provider: "tradier" });

//             if (isOCC) {
//               const optSide = inferSideServer(q.symbol, price);
//               const book = midBySym.get(q.symbol) || {};
//               const eps = epsFor(book);
//               let at: "bid" | "ask" | "mid" | "between" = "between";
//               if (book.ask && price >= book.ask - eps) at = "ask";
//               else if (book.bid && price <= book.bid + eps) at = "bid";
//               else if (book.mid && Math.abs(price - book.mid) <= eps) at = "mid";

//               const seenVol = Number(q.volume ?? NaN);
//               let priorVol = volByOcc.has(q.symbol) ? volByOcc.get(q.symbol) : 0;
//               if (Number.isFinite(seenVol)) priorVol = Math.max(0, seenVol - qty);
//               if (Number.isFinite(seenVol)) volByOcc.set(q.symbol, seenVol);
//               else volByOcc.set(q.symbol, (volByOcc.get(q.symbol) || 0) + qty);
//               const oi = oiByOcc.get(q.symbol) ?? null;

//               const { action, action_conf } = classifyOpenClose({
//                 qty,
//                 oi,
//                 priorVol,
//                 side: optSide.side,
//                 at,
//               });

//               optionTsCnt++;
//               if (optionTsCnt % 100 === 1) {
//                 console.log("[SRV option_ts QUOTE]", q.symbol, {
//                   qty,
//                   price,
//                   side: optSide.side,
//                   at,
//                   action,
//                   priorVol,
//                   oi,
//                 });
//               }

//               broadcast({
//                 type: "option_ts",
//                 symbol: q.symbol,
//                 provider: "tradier",
//                 data: {
//                   id: `ots_${q.symbol}_${q.trade_time || Date.now()}`,
//                   ts: Date.now(),
//                   option: {
//                     expiry: "",
//                     strike: 0,
//                     right: /C\d{8}$/i.test(q.symbol) ? "C" : "P",
//                   },
//                   qty,
//                   price,
//                   side: optSide.side,
//                   side_src: optSide.side_src,
//                   oi,
//                   priorVol,
//                   book,
//                   at,
//                   action,
//                   action_conf,
//                 },
//               });
//               return;
//             }

//             // non-OCC “trade-like quote”: already broadcast as quotes
//           } else {
//             // NBBO-only update
//             broadcast({ type: "quotes", data: q, provider: "tradier" });
//           }

//           return;
//         }
//       } catch (err) {
//         console.warn("[SSE parse error]", err);
//       }
//     };

//     es.onerror = () => {
//       console.warn(
//         `[SSE error] restarting. totals: quote=${qCnt}, trade=${tCnt}, occQ=${occQuoteCnt}, occT=${occTradeCnt}, option_ts=${optionTsCnt}`
//       );
//       try { es.close(); } catch {}
//       setTimeout(() => startQuoteStream(uniq), 1500);
//     };
//   } catch (e) {
//     console.error("startQuoteStream error:", errToJson(e));
//   }
// }
/* ------------------------ streaming quotes (robust JS) ------------------------ */
var esQuotes = null;

function startQuoteStream(symbolsOrOCC = []) {
  try {
    if (esQuotes) esQuotes.close();
    const uniq = [...new Set(symbolsOrOCC)].filter(Boolean);
    if (uniq.length === 0) return;

    const url = `${TRADIER_BASE}/markets/events?symbols=${encodeURIComponent(
      uniq.join(",")
    )}&sessionid=${Date.now()}`;
    const es = new EventSource(url, { headers: H_SSE() });
    esQuotes = es;

    // --- DEBUG counters ---
    let qCnt = 0, tCnt = 0, occQuoteCnt = 0, occTradeCnt = 0, optionTsCnt = 0;

    es.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);

        // increment counters (no ++(ternary))
        if (msg && msg.type === "quote") qCnt++;
        else if (msg && msg.type === "trade") tCnt++;
        else qCnt++;

        // sample log every 200th message of that type
        const cur = msg && msg.type === "quote" ? qCnt : msg && msg.type === "trade" ? tCnt : qCnt;
        if (msg && msg.type && cur % 200 === 1) {
          console.log(`[SSE ${msg.type}] sample`, (msg.data && (msg.data.symbol || msg.symbol)) || "", { qCnt, tCnt });
        }

        // ================== TRADE path ==================
        if (msg && msg.type === "trade" && msg.data) {
          const tr = msg.data;
          const sym = tr.symbol;
          const isOCC = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(sym);
          if (isOCC) occTradeCnt++;

          const price = Number(tr.price ?? tr.last ?? tr.p ?? 0);
          const qty   = Number(tr.size ?? tr.s ?? tr.quantity ?? tr.last_size ?? 0);

          // refresh NBBO cache if bid/ask present
          if (tr.bid != null || tr.ask != null) updateMidFromQuote(tr);

          if (isOCC && price > 0 && qty > 0) {
            const sideInfo = inferSideServer(sym, price);
            const book = midBySym.get(sym) || {};
            const eps = epsFor(book);
            let at = "between";
            if (book.ask && price >= book.ask - eps) at = "ask";
            else if (book.bid && price <= book.bid + eps) at = "bid";
            else if (book.mid && Math.abs(price - book.mid) <= eps) at = "mid";

            // update rolling volume for action heuristic
            const seenVol = Number(tr.volume ?? NaN);
            let priorVol = volByOcc.has(sym) ? volByOcc.get(sym) : 0;
            if (Number.isFinite(seenVol)) priorVol = Math.max(0, seenVol - qty);
            if (Number.isFinite(seenVol)) volByOcc.set(sym, seenVol);
            else volByOcc.set(sym, (volByOcc.get(sym) || 0) + qty);
            const oi = oiByOcc.get(sym) ?? null;

            const ac = classifyOpenClose({
              qty,
              oi,
              priorVol,
              side: sideInfo.side,
              at,
            });

            optionTsCnt++;
            if (optionTsCnt % 100 === 1) {
              console.log("[SRV option_ts TRADE]", sym, { qty, price, side: sideInfo.side, at, action: ac.action, priorVol, oi });
            }

            broadcast({
              type: "option_ts",
              symbol: sym,
              provider: "tradier",
              data: {
                id: `ots_${sym}_${tr.trade_time || Date.now()}`,
                ts: Date.now(),
                option: { expiry: "", strike: 0, right: /C\d{8}$/i.test(sym) ? "C" : "P" },
                qty,
                price,
                side: sideInfo.side,
                side_src: sideInfo.side_src,
                oi,
                priorVol,
                book,
                at,
                action: ac.action,
                action_conf: ac.action_conf,
              },
            });
            return;
          }
          // non-OCC trades: let quotes path handle equities context if needed
        }

        // ================== QUOTE path ==================
        if (msg && msg.type === "quote" && msg.data) {
          const q = msg.data;

          // always refresh NBBO cache when bid/ask present
          if (q.bid != null || q.ask != null) updateMidFromQuote(q);

          const isOCC = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(q.symbol);
          if (isOCC) occQuoteCnt++;

          // trade-like data sometimes rides on quote frames
          const price = Number(q.last ?? q.price ?? q.p ?? 0);
          const qty   = Number(q.size ?? q.trade_size ?? q.last_size ?? q.quantity ?? 0);

          if (price > 0 && qty > 0) {
            // broadcast quotes frame (equity prints + side on root)
            const rootSym = isOCC ? q.symbol.replace(/\d{6}[CP]\d{8}$/, "") : q.symbol;
            const sideInfoRoot = inferSideServer(rootSym, price);
            broadcast({ type: "quotes", data: q, side: sideInfoRoot.side, side_src: sideInfoRoot.side_src, provider: "tradier" });

            if (isOCC) {
              const sideInfo = inferSideServer(q.symbol, price);
              const book = midBySym.get(q.symbol) || {};
              const eps = epsFor(book);
              let at = "between";
              if (book.ask && price >= book.ask - eps) at = "ask";
              else if (book.bid && price <= book.bid + eps) at = "bid";
              else if (book.mid && Math.abs(price - book.mid) <= eps) at = "mid";

              const seenVol = Number(q.volume ?? NaN);
              let priorVol = volByOcc.has(q.symbol) ? volByOcc.get(q.symbol) : 0;
              if (Number.isFinite(seenVol)) priorVol = Math.max(0, seenVol - qty);
              if (Number.isFinite(seenVol)) volByOcc.set(q.symbol, seenVol);
              else volByOcc.set(q.symbol, (volByOcc.get(q.symbol) || 0) + qty);
              const oi = oiByOcc.get(q.symbol) ?? null;

              const ac = classifyOpenClose({
                qty,
                oi,
                priorVol,
                side: sideInfo.side,
                at,
              });

              optionTsCnt++;
              if (optionTsCnt % 100 === 1) {
                console.log("[SRV option_ts QUOTE]", q.symbol, { qty, price, side: sideInfo.side, at, action: ac.action, priorVol, oi });
              }

              broadcast({
                type: "option_ts",
                symbol: q.symbol,
                provider: "tradier",
                data: {
                  id: `ots_${q.symbol}_${q.trade_time || Date.now()}`,
                  ts: Date.now(),
                  option: { expiry: "", strike: 0, right: /C\d{8}$/i.test(q.symbol) ? "C" : "P" },
                  qty,
                  price,
                  side: sideInfo.side,
                  side_src: sideInfo.side_src,
                  oi,
                  priorVol,
                  book,
                  at,
                  action: ac.action,
                  action_conf: ac.action_conf,
                },
              });
              return;
            }

            // non-OCC trade-like quote already broadcast above as "quotes"
          } else {
            // NBBO-only refresh
            broadcast({ type: "quotes", data: q, provider: "tradier" });
          }

          return;
        }
      } catch (err) {
        console.warn("[SSE parse error]", err);
      }
    };

    es.onerror = () => {
      console.warn(
        `[SSE error] restarting. totals: quote=${qCnt}, trade=${tCnt}, occQ=${occQuoteCnt}, occT=${occTradeCnt}, option_ts=${optionTsCnt}`
      );
      try { es.close(); } catch {}
      setTimeout(() => startQuoteStream(uniq), 1500);
    };
  } catch (e) {
    console.error("startQuoteStream error:", errToJson(e));
  }
}

// let esQuotes = null;
// function startQuoteStream(symbolsOrOCC=[]) {
//   try {
//     if (esQuotes) esQuotes.close();
//     const uniq = [...new Set(symbolsOrOCC)].filter(Boolean);
//     if (uniq.length === 0) return;
//     const url = `${TRADIER_BASE}/markets/events?symbols=${encodeURIComponent(uniq.join(","))}&sessionid=${Date.now()}`;
//     const es  = new EventSource(url, { headers: H_SSE() }); esQuotes = es;

//     es.onmessage = (e) => {
//       try {
//         const msg = JSON.parse(e.data);
//         if (msg?.type === "quote" && msg?.data) {
//           const q = msg.data;
//           updateMidFromQuote(q);

//           if (q.last && q.size && q.size > 0) {
//             const rootSym = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(q.symbol)
//               ? q.symbol.replace(/\d{6}[CP]\d{8}$/, "")
//               : q.symbol;

//             const price = Number(q.last);
//             const { side, side_src } = inferSideServer(rootSym, price);
//             lastTradeBySym.set(rootSym, price);
//             broadcast({ type: "quotes", data: q, side, side_src });

//             const isOCC = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(q.symbol);
//             if (isOCC) {
//               const optSide = inferSideServer(q.symbol, price);
//               const book = midBySym.get(q.symbol) || {};
//               const eps = epsFor(book);
//               let at = "between";
//               if (book.ask && price >= book.ask - eps) at = "ask";
//               else if (book.bid && price <= book.bid + eps) at = "bid";
//               else if (book.mid && Math.abs(price - book.mid) <= eps) at = "mid";

//               const seenVol = Number(q.volume ?? NaN);
//               let priorVol = volByOcc.has(q.symbol) ? volByOcc.get(q.symbol) : 0;
//               if (Number.isFinite(seenVol)) priorVol = Math.max(0, seenVol - Number(q.size));
//               if (Number.isFinite(seenVol)) volByOcc.set(q.symbol, seenVol);
//               else volByOcc.set(q.symbol, (volByOcc.get(q.symbol) || 0) + Number(q.size));
//               const oi = oiByOcc.get(q.symbol) ?? null;

//               const { action, action_conf } = classifyOpenClose({
//                 qty: Number(q.size), oi, priorVol, side: optSide.side, at
//               });

//               broadcast({
//                 type: "option_ts",
//                 symbol: q.symbol,
//                 data: {
//                   id: `ots_${q.symbol}_${q.trade_time || Date.now()}`,
//                   ts: Date.now(),
//                   option: { expiry: "", strike: 0, right: /C\d{8}$/i.test(q.symbol) ? "C" : "P" },
//                   qty: q.size,
//                   price,
//                   side: optSide.side,
//                   side_src: optSide.side_src,
//                   oi, priorVol, book, at,
//                   action, action_conf
//                 }
//               });
//             }
//           } else {
//             broadcast({ type: "quotes", data: q });
//           }
//         }
//       } catch { /* ignore */ }
//     };
//     es.onerror = () => { es.close(); setTimeout(() => startQuoteStream(uniq), 1500); };
//   } catch (e) {
//     console.error("startQuoteStream error:", errToJson(e));
//   }
// }


let stopCurrentWatch = null;

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// app.get("/watch", async (req, res) => {
//   try {
//     const provider = String(req.query.provider || "tradier").toLowerCase(); // tradier|alpaca|both
//     const symbols = String(req.query.symbols || "SPY").split(",").map(s=>s.trim().toUpperCase()).filter(Boolean);
//     const eqForTS = String(req.query.eqForTS || symbols.join(",")).split(",").map(s=>s.trim().toUpperCase()).filter(Boolean);
//     const backfillMins = req.query.backfill ? Number(req.query.backfill) : 5;
//     const day = parseDay(req);

//     const moneyness = req.query.moneyness ? Number(req.query.moneyness) : 0.25;
//     const limit     = req.query.limit ? Number(req.query.limit) : 150;
//     const expiries  = String(req.query.expiries || "").split(",").map(s=>s.trim()).filter(Boolean);

//     // cancel any previous loops
//     if (typeof stopCurrentWatch === "function") {
//       try { stopCurrentWatch(); } catch {}
//       stopCurrentWatch = null;
//     }

//     console.log(`[watch] provider=${provider} syms=${symbols.join(",")} eqForTS=${eqForTS.join(",")} backfill=${backfillMins} mny=${moneyness} limit=${limit}`);

//     if (provider === "polygon") {
//       // start polygon polling loops
//       stopCurrentWatch = await startPolygonWatch({
//         equities: symbols,
//         moneyness,
//         limit,
//         broadcast,
//         classifyOptionAction: classifyOpenClose,
//       });

//       return res.json({
//         ok: true,
//         provider: "polygon",
//         env: { provider: "polygon", base: "https://api.polygon.io", delayed: true },
//         watching: { equities: symbols, eqForTS, day, backfillMins, limit, moneyness, options_count: 0 }
//       });
//     }

//     // Build options watch list using Tradier (best for options detail)
//     const occSet = new Set();
//     for (const s of symbols) {
//       try {
//         const occ = await buildOptionsWatchList(s, { expiries, moneyness, limit: Math.ceil(limit / symbols.length) });
//         occ.forEach(x => occSet.add(x));
//       } catch (e) { console.warn("buildOptionsWatchList failed", s, errToJson(e)); }
//     }

//     // ----- start streams based on provider -----
//     if (provider === "tradier" || provider === "both") {
//       startQuoteStream([...symbols, ...occSet]); // your existing Tradier SSE -> emits quotes/equity_ts/option_ts w/ provider implicit
//     }

//     // Lightweight Alpaca flow for EQUITIES (options flow varies by plan; keep options via Tradier)
//     let alpTimer = null;
//     if (provider === "alpaca" || provider === "both") {
//       const uniqRoots = [...new Set(symbols)];
//       const T = 1250; // poll roughly every 1.25s
//       alpTimer = setInterval(() => pollAlpacaLatestTrades(uniqRoots), T);
//       // attach to req “session” so it can be cleared if needed; for simplicity we let it run
//     }

//     // backfill (equity prints) for charts/initial rows — from Tradier
//     if (backfillMins > 0) {
//       await Promise.all(eqForTS.map(sym => backfillEquityTS(sym, day, backfillMins)));
//     }

//     res.json({
//       ok: true,
//       provider,
//       env: { provider, base: TRADIER_BASE, sandbox: isSandbox, ts_interval: TS_INTERVAL },
//       watching:{ equities: symbols, options_count: occSet.size, eqForTS, day, backfillMins, limit, moneyness }
//     });
    
//   } catch (e) {
//     const detail = errToJson(e);
//     console.error("/watch error:", detail);
//     res.status(detail.status || 500).json({ ok:false, error: detail });
//   }
// });
// --- Market-hours helper (New York time, regular session only) ---
function isMarketOpenNY() {
  const now = DateTime.now().setZone("America/New_York");
  if (now.weekday === 6 || now.weekday === 7) return false; // Sat/Sun
  const mins = now.hour * 60 + now.minute;
  return mins >= (9 * 60 + 30) && mins < (16 * 60); // 09:30–16:00 ET
}

// --- Historical equity ticks (Tradier timesales) ---
async function getEquityTimesales(symbol, dayISO, minutes) {
  try {
    const start = DateTime.fromISO(dayISO + "T09:30:00", { zone: "America/New_York" });
    const end   = start.plus({ minutes: Math.max(1, Number(minutes) || 5) });

    const params = {
      symbol,
      interval: TS_INTERVAL, // your existing: "tick" (prod) or "1min" (sandbox)
      start: start.toFormat("yyyy-LL-dd HH:mm:ss"),
      end:   end.toFormat("yyyy-LL-dd HH:mm:ss"),
    };

    const { data } = await axios.get(`${TRADIER_BASE}/markets/timesales`, {
      headers: H_JSON(), params
    });

    const arr = (data && data.series && data.series.data) ? data.series.data : [];
    return arr.map(t => ({
      time:  t.time,                                                     // 'YYYY-MM-DD HH:mm:ss'
      price: Number(t.price ?? t.last ?? t.close ?? 0),
      size:  Number(t.size ?? t.volume ?? t.qty ?? t.quantity ?? 0),
      bid:   Number.isFinite(+t.bid) ? +t.bid : undefined,
      ask:   Number.isFinite(+t.ask) ? +t.ask : undefined,
    })).filter(r => r.price > 0 && r.size > 0);
  } catch (e) {
    console.error("getEquityTimesales error:", errToJson(e));
    return [];
  }
}
// --- Session window helpers (NY time strings) ---
function nyDayStartISO(day) { return `${day} 09:30:00`; }
function nyDayEndISO(day)   { return `${day} 16:00:00`; }

// --- Chunked Tradier time-sales fetch to avoid 502 "Body buffer overflow" ---
async function fetchTimesalesChunked({ symbol, day, interval = "1min", startISO, endISO }) {
  const start = startISO ? new Date(startISO) : new Date(nyDayStartISO(day));
  const end   = endISO   ? new Date(endISO)   : new Date(nyDayEndISO(day));
  const stepMin = interval === "tick" ? 5 : 120; // small for tick, bigger for 1min

  const frames = [];
  for (let t = new Date(start); t < end; ) {
    const tEnd = new Date(Math.min(end.getTime(), t.getTime() + stepMin*60*1000));
    try {
      const { data } = await axios.get(`${TRADIER_BASE}/v1/markets/timesales`, {
        headers: H_JSON(),
        params: {
          symbol,
          interval,
          start: t.toISOString().slice(0,19).replace('T',' '),
          end:   tEnd.toISOString().slice(0,19).replace('T',' ')
        }
      });
      const arr = data?.series?.data || [];
      frames.push(...arr);
    } catch (e) {
      console.warn("[timesales chunk error]", symbol, interval, { start: t, end: tEnd }, errToJson(e));
      // optional: add a retry/backoff if you like
    }
    t = tEnd;
  }
  return frames;
}
// --- Replay equity ticks as if live (drip frames out) ---
// function replayEquityTimesales(symbol, rows, speedMs) {
//   const pace = Math.max(10, Number(speedMs) || 100); // default 100ms per print
//   let i = 0;

//   const tick = () => {
//     if (i >= rows.length) return;
//     const r = rows[i++];

//     // existing book/tick side logic
//     const sidePack = inferSideServer(symbol, r.price);
//     lastTradeBySym.set(symbol, r.price);

//     const book = midBySym.get(symbol) || {};
//     const eps  = epsFor(book);
//     let at = "between";
//     if (book.ask && r.price >= book.ask - eps) at = "ask";
//     else if (book.bid && r.price <= book.bid + eps) at = "bid";
//     else if (book.mid && Math.abs(r.price - book.mid) <= eps) at = "mid";

//     broadcast({
//       type: "equity_ts",
//       symbol,
//       data: {
//         time: r.time,
//         price: r.price,
//         size: r.size,
//         bid: r.bid,
//         ask: r.ask,
//         side: sidePack.side,
//         side_src: sidePack.side_src,
//         at,
//       }
//     });

//     setTimeout(tick, pace);
//   };

//   tick();
// }
let stopReplay = null;

async function replayEquityTimesales({ symbols = [], day, minutes = 390, speed = 60 }) {
  const frames = [];

  for (const sym of symbols) {
    const all = await fetchTimesalesChunked({
      symbol: sym,
      day,
      interval: "1min",                 // never whole-day tick
      startISO: nyDayStartISO(day),
      endISO:   nyDayEndISO(day)
    });

    const arr = all.slice(0, Math.max(1, Math.min(minutes, all.length)));
    for (const bar of arr) {
      const price = Number(bar.close ?? bar.price ?? bar.last ?? 0);
      const size  = Number(bar.volume ?? 1);
      if (!(price > 0 && size > 0)) continue;

      const { side, side_src } = inferSideServer(sym, price);
      frames.push({
        type: "equity_ts",
        symbol: sym,
        data: { time: bar.time, price, size, side, side_src, at: "mid" }
      });
    }
  }

  frames.sort((a,b) => new Date(a.data.time) - new Date(b.data.time));
  console.log(`[replay] scheduling ${frames.length} frames @${speed}ms`);

  let i = 0;
  const timer = setInterval(() => {
    if (i >= frames.length) { clearInterval(timer); console.log("[replay] done"); return; }
    broadcast(frames[i++]);
  }, speed);

  return () => clearInterval(timer);
}

// app.get("/watch", async (req, res) => {
//   try {
//     const provider = String(req.query.provider || "tradier").toLowerCase();

//     const symbols = String(req.query.symbols || "SPY")
//       .split(",").map(s => s.trim().toUpperCase()).filter(Boolean);

//     const eqForTS = String(req.query.eqForTS || symbols.join(","))
//       .split(",").map(s => s.trim().toUpperCase()).filter(Boolean);

//     // Trading day to use (defaults to your parseDay, which handles weekends)
//     const day = (req.query.day && /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.day)))
//       ? String(req.query.day)
//       : parseDay(req);

//     // --- Smart defaults for live/replay ---
//     let live   = req.query.live === "0" ? 0 : (req.query.live === "1" ? 1 : null);
//     let replay = req.query.replay === "1" ? 1 : (req.query.replay === "0" ? 0 : null);

//     if (live === null && replay === null) {
//       live = isMarketOpenNY() ? 1 : 0;
//       replay = live ? 0 : 1;
//     }

//     // minutes/backfill tuned by mode
//     const minutes = req.query.minutes ? Number(req.query.minutes)
//                   : (live ? 0 : 390);
//     const backfillMins = req.query.backfill ? Number(req.query.backfill)
//                        : (live ? 10 : 0);

//     const moneyness = Number(req.query.moneyness || 0.25);
//     const limit     = Number(req.query.limit     || 150);
//     const expiries  = String(req.query.expiries || "")
//                         .split(",").map(s => s.trim()).filter(Boolean);

//     // stop prior watchers
//     if (typeof stopCurrentWatch === "function") {
//       try { stopCurrentWatch(); } catch {}
//       stopCurrentWatch = null;
//     }

//     // ---- Build OCC set (options universe) using your existing helper ----
//     const occSet = new Set();
//     for (const s of symbols) {
//       try {
//         const occ = await buildOptionsWatchList(
//           s,
//           { expiries, moneyness, limit: Math.ceil(limit / Math.max(1, symbols.length)) }
//         );
//         occ.forEach(x => occSet.add(x));
//       } catch (e) {
//         console.warn("buildOptionsWatchList failed", s, errToJson(e));
//       }
//     }

//     // ---- EQUITIES: replay/backfill for selected session ----
//     if (!live && minutes > 0) {
//       for (const sym of eqForTS) {
//         const rows = await getEquityTimesales(sym, day, minutes);
//         // drip them out like a tape (speed param optional)
//         const speedMs = Number(req.query.speed || 60);
//         replayEquityTimesales(sym, rows, speedMs);
//       }
//     } else if (live && backfillMins > 0) {
//       await Promise.all(eqForTS.map(sym => backfillEquityTS(sym, day, backfillMins)));
//     }

//     // ---- OPTIONS: optional light seeding off-hours so UI isn’t empty ----
//     if (!live) {
//       for (const occ of Array.from(occSet).slice(0, 100)) {
//         const book = midBySym.get(occ) || {};
//         const px = Number.isFinite(book.mid) ? book.mid : (book.ask ?? book.bid ?? 0);
//         if (!(px > 0)) continue;
//         const sidePack = inferSideServer(occ, px);
//         const oi = oiByOcc.get(occ) ?? null;
//         const priorVol = volByOcc.get(occ) ?? 0;

//         broadcast({
//           type: "option_ts",
//           symbol: occ,
//           data: {
//             id: `ots_${occ}_${day}`,
//             ts: Date.now(),
//             option: { expiry: "", strike: 0, right: /C\d{8}$/i.test(occ) ? "C" : "P" },
//             qty: 1, price: px,
//             side: sidePack.side, side_src: sidePack.side_src,
//             oi, priorVol, book, at: "between",
//             action: "—", action_conf: "low"
//           }
//         });
//       }
//     }

//     // ---- Live streaming path (unchanged) ----
//     if (live) {
//       if (provider === "polygon") {
//         stopCurrentWatch = await startPolygonWatch({
//           equities: symbols,
//           moneyness,
//           limit,
//           broadcast,
//           classifyOptionAction: classifyOpenClose,
//         });
//       } else {
//         // Tradier SSE (quotes -> equity_ts/option_ts via your current logic)
//         startQuoteStream([ ...symbols, ...occSet ]);
//       }
//     }

//     return res.json({
//       ok: true,
//       provider,
//       env: { provider, base: TRADIER_BASE, sandbox: isSandbox, ts_interval: TS_INTERVAL },
//       watching: { equities: symbols, options_count: occSet.size, eqForTS, day, minutes, backfillMins, live, replay, limit, moneyness }
//     });
//   } catch (e) {
//     const detail = errToJson(e);
//     console.error("/watch error:", detail);
//     res.status(detail.status || 500).json({ ok:false, error: detail });
//   }
// });

app.get("/watch", async (req, res) => {
  try {
    const provider = String(req.query.provider || "tradier").toLowerCase();
    const symbols  = String(req.query.symbols || "SPY").split(",").map(s => s.trim().toUpperCase()).filter(Boolean);
    const eqForTS  = String(req.query.eqForTS || symbols.join(",")).split(",").map(s => s.trim().toUpperCase()).filter(Boolean);

    const live    = req.query.live === "1";     // live mode
    const replay  = req.query.replay === "1";   // replay mode
    const minutes = Number(req.query.minutes || 390);
    const speed   = Number(req.query.speed || 60);
    const backfillMins = Number(req.query.backfill || 0);

    // stop any prior runs
    if (typeof stopCurrentWatch === "function") { try { stopCurrentWatch(); } catch {} stopCurrentWatch = null; }
    if (typeof stopReplay === "function")       { try { stopReplay(); }       catch {} stopReplay = null; }

    if (live) {
      const day = parseDay(req);
      if (backfillMins > 0) {
        await Promise.all((eqForTS.length ? eqForTS : symbols).map(sym => backfillEquityTS(sym, day, backfillMins)));
      }
      // start your existing live streams (quotes/SSE/etc.)
      // startQuoteStream([...symbols, ...occSet]);  // keep your logic here
      return res.json({ ok: true, mode: "live", provider, env:{ provider }, watching: { symbols, eqForTS } });
    }

    if (replay) {
      const day = parseDay(req);
      stopReplay = await replayEquityTimesales({
        symbols: eqForTS.length ? eqForTS : symbols,
        day, minutes, speed
      });
      return res.json({ ok: true, mode: "replay", provider, day, minutes, speed, watching: { symbols, eqForTS } });
    }

    // default: treat as live
    return res.json({ ok: true, mode: "live-default", provider, watching: { symbols, eqForTS } });
  } catch (e) {
    const detail = errToJson(e);
    console.error("/watch error:", detail);
    res.status(detail.status || 500).json({ ok:false, error: detail });
  }
});

/*
let stopCurrentWatch = null; // allow switching providers without restarting the server

app.get("/watch", async (req, res) => {
  try {
    console.log(
      `[${req.id}] /watch symbols=${symbols.join(",")} eqForTS=${eqForTS.join(",")} provider=${provider} backfill=${backfillMins} mny=${moneyness} limit=${limit}`
    );
    const provider = String(req.query.provider || "tradier").toLowerCase();

    const symbols   = String(req.query.symbols || "SPY")
      .split(",").map(s=>s.trim().toUpperCase()).filter(Boolean);

    const eqForTS   = String(req.query.eqForTS || symbols.join(","))
      .split(",").map(s=>s.trim().toUpperCase()).filter(Boolean);

    const backfillMins = req.query.backfill ? Number(req.query.backfill) : 5;
    const day          = parseDay(req);

    const moneyness = req.query.moneyness ? Number(req.query.moneyness) : 0.25;
    const limit     = req.query.limit ? Number(req.query.limit) : 150;
    const expiries  = String(req.query.expiries || "").split(",").map(s=>s.trim()).filter(Boolean);

    // stop any prior loops when switching providers
    if (typeof stopCurrentWatch === "function") {
      try { stopCurrentWatch(); } catch {}
      stopCurrentWatch = null;
    }

    if (provider === "polygon") {
      // === Polygon (15-min delayed on lower tiers): poll quotes/trades and broadcast frames ===
      stopCurrentWatch = await startPolygonWatch({
        equities: symbols,
        moneyness,
        limit,
        broadcast,                 // your existing broadcast(msg)
        classifyOptionAction: classifyOpenClose, // reuse your BTO/BTC/STO/STC heuristic
      });

      return res.json({
        ok: true,
        env: { provider: "polygon", base: "https://api.polygon.io", delayed: true },
        watching: { equities: symbols, eqForTS, day, backfillMins, limit, moneyness, options_count: 0 }
      });
    }

    // === Default: Tradier real-time (your existing path) ===
    const occSet = new Set();
    for (const s of symbols) {
      try {
        const occ = await buildOptionsWatchList(
          s,
          { expiries, moneyness, limit: Math.ceil(limit / Math.max(1, symbols.length)) }
        );
        occ.forEach(x => occSet.add(x));
      } catch (e) {
        console.warn("buildOptionsWatchList failed", s, errToJson(e));
      }
    }

    // start real-time SSE quotes (equities + options OCCs)
    startQuoteStream([...symbols, ...occSet]);

    // optional equity backfill from Tradier timesales
    if (backfillMins > 0) {
      await Promise.all(eqForTS.map(sym => backfillEquityTS(sym, day, backfillMins)));
    }

    console.log(
      `[${req.id}] /watch OK -> ${provider} equities=${symbols.length} opts=${provider === 'polygon' ? 0 : occSet.size}`
    );

    return res.json({
      ok: true,
      env: { provider: "tradier", base: TRADIER_BASE, sandbox: isSandbox, ts_interval: TS_INTERVAL },
      watching: { equities: symbols, options_count: occSet.size, eqForTS, day, backfillMins, limit, moneyness }
    });

  } catch (e) {
    const detail = errToJson(e);
    console.error("/watch error:", detail);
    res.status(detail.status || 500).json({ ok:false, error: detail });
  }
});
*/

/* ------------------------ /watch (existing) ------------------------
app.get("/watch", async (req, res) => {
  try {
    const provider = String(req.query.provider || "tradier").toLowerCase();
    const symbols = String(req.query.symbols || "SPY").split(",").map(s=>s.trim().toUpperCase()).filter(Boolean);
    const eqForTS = String(req.query.eqForTS || symbols.join(",")).split(",").map(s=>s.trim().toUpperCase()).filter(Boolean);
    const backfillMins = req.query.backfill ? Number(req.query.backfill) : 5;
    const day = parseDay(req);

    const moneyness = req.query.moneyness ? Number(req.query.moneyness) : 0.25;
    const limit     = req.query.limit ? Number(req.query.limit) : 150;
    const expiries  = String(req.query.expiries || "").split(",").map(s=>s.trim()).filter(Boolean);

    const occSet = new Set();
    for (const s of symbols) {
      try {
        const occ = await buildOptionsWatchList(s, { expiries, moneyness, limit: Math.ceil(limit / symbols.length) });
        occ.forEach(x => occSet.add(x));
      } catch (e) { console.warn("buildOptionsWatchList failed", s, errToJson(e)); }
    }

    startQuoteStream([...symbols, ...occSet]);

    if (backfillMins > 0) {
      await Promise.all(eqForTS.map(sym => backfillEquityTS(sym, day, backfillMins)));
    }

    res.json({ ok:true, env:{ base:TRADIER_BASE, sandbox:isSandbox, ts_interval:TS_INTERVAL },
      watching:{ equities:symbols, options_count:occSet.size, eqForTS, day, backfillMins, limit } });
  } catch (e) {
    const detail = errToJson(e); console.error("/watch error:", detail);
    res.status(detail.status || 500).json({ ok:false, error: detail });
  }
});
*/

/* ======================== NEW: SCANNERS ======================== */

/** Curated, liquid “popular” roots */

// --- Dynamic "popular" roots, sourced from Alpaca most-actives by trades ---
const POPULAR_TTL_MS = 5 * 60 * 1000; // 5 minutes

// Fallback if Alpaca fails (never used when Alpaca works)
const POPULAR_FALLBACK = [
  "SPY","QQQ","IWM",
  "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","AMD","NFLX","AVGO",
  "JPM","BAC","XOM","CVX","WMT","COST","UNH","MRK","PFE",
  "SMCI","NIO","RIOT","COIN","PLTR","UBER","SHOP","CRM","ORCL"
];

// let POPULAR_CACHE = { symbols: POPULAR_FALLBACK, ts: 0 };

let POPULAR_CACHE = { symbols: POPULAR_FALLBACK, ts: 0, meta: { reason: "fallback-init" } };

async function fetchPopularFromAlpaca(by = 'trades', top = 40) {
  const resp = await ALPACA.get('/v1beta1/screener/stocks/most-actives', {
    params: { by, top: String(top) },
    validateStatus: () => true,
  });
  if (resp.status >= 400 || resp.data?.message) {
    const msg = resp.data?.message || `HTTP ${resp.status}`;
    // Surface this to caller
    const err = new Error(`alpaca screener error: ${msg}`);
    err.upstream = { status: resp.status, data: resp.data };
    throw err;
  }
  const rows = resp.data?.most_actives || [];
  const syms = rows.map(r => r.symbol).filter(Boolean);
  return Array.from(new Set(syms)).slice(0, top);
}

async function getPopularRoots(opts = {}) {
  const { force = false, by = 'trades', top = 40 } = opts;
  const now = Date.now();
  if (!force && (now - POPULAR_CACHE.ts < POPULAR_TTL_MS) && POPULAR_CACHE.symbols?.length) {
    return { symbols: POPULAR_CACHE.symbols, cached: true, meta: POPULAR_CACHE.meta };
  }
  try {
    const symbols = await fetchPopularFromAlpaca(by, top);
    POPULAR_CACHE = { symbols, ts: now, meta: { by, top, source: "alpaca" } };
    return { symbols, cached: false, meta: POPULAR_CACHE.meta };
  } catch (e) {
    // keep previous cache if we have one; otherwise fallback list
    const symbols = POPULAR_CACHE.symbols?.length ? POPULAR_CACHE.symbols : POPULAR_FALLBACK;
    POPULAR_CACHE = { symbols, ts: POPULAR_CACHE.ts || 0, meta: { by, top, source: "fallback", error: e?.message, upstream: e?.upstream } };
    return { symbols, cached: true, meta: POPULAR_CACHE.meta };
  }
}

/**
async function fetchPopularFromAlpaca(by = 'trades', top = 40) {
  if (!H_ALPACA) throw new Error('alpaca keys missing');
  const url = `${ALPACA_BASE}/v1beta1/screener/stocks/most-actives`;
  const resp = await axios.get(url, {
    headers: { ...H_ALPACA, Accept: 'application/json' },
    params: { by, top: String(top) }, // by: volume|trades
    validateStatus: () => true,
  });
  if (resp.status >= 400 || resp.data?.message) {
    const msg = resp.data?.message || `HTTP ${resp.status}`;
    throw new Error(`alpaca screener error: ${msg}`);
  }
  const rows = resp.data?.most_actives || [];
  const syms = rows.map(r => r.symbol).filter(Boolean);
  // De-dup and keep liquid-ish count
  return Array.from(new Set(syms)).slice(0, top);
}
*/

/**
async function getPopularRoots() {
  const now = Date.now();
  if (now - POPULAR_CACHE.ts < POPULAR_TTL_MS && POPULAR_CACHE.symbols?.length) {
    return POPULAR_CACHE.symbols;
  }
  try {
    const symbols = await fetchPopularFromAlpaca('trades', 40);
    POPULAR_CACHE = { symbols, ts: now };
    return symbols;
  } catch (e) {
    // keep previous cache if it exists; otherwise fallback
    if (POPULAR_CACHE.symbols?.length) return POPULAR_CACHE.symbols;
    return POPULAR_FALLBACK;
  }
}
*/

// --- the route (put with your other routes, before app.listen) ---

/**
app.get('/popular', async (_req, res) => {
  try {
    const symbols = await getPopularRoots();
    res.json({ ok: true, ts: POPULAR_CACHE.ts, symbols, source: (POPULAR_CACHE.ts ? 'cache/alpaca' : 'fallback') });
  } catch (e) {
    res.status(500).json({ ok:false, error: e?.message || 'failed' });
  }
});
*/
// replace your /popular route with this one
app.get('/popular', async (req, res) => {
  try {
    const provider = String(req.query.provider || "alpaca").toLowerCase();
    let symbols1 = [];

    if (provider === "polygon") {
      try {
        const act = await polyMostActives({ by: "trade_count", top: 40 });
        symbols1 = act.map(x => x.symbol);
        return res.json({ ok: true, ts: Date.now(), source: "polygon", symbols1 });
      } catch {
        // fall through to your existing cache/fallback
        const force = req.query.force === '1';
        let by = String(req.query.by || 'trades').toLowerCase();
        if (!['trades','volume'].includes(by)) by = 'trades';
        const top = Math.max(1, Math.min(100, Number(req.query.top || 40)));

        const { symbols, cached, meta } = await getPopularRoots({ force, by, top });
        res.json({ ok: true, ts: POPULAR_CACHE.ts, cached, symbols, meta });        
      }
    }    
    const force = req.query.force === '1';
    let by = String(req.query.by || 'trades').toLowerCase();
    if (!['trades','volume'].includes(by)) by = 'trades';
    const top = Math.max(1, Math.min(100, Number(req.query.top || 40)));

    const { symbols, cached, meta } = await getPopularRoots({ force, by, top });
    res.json({ ok: true, ts: POPULAR_CACHE.ts, cached, symbols, meta });
  } catch (e) {
    res.status(500).json({ ok:false, error: e?.message || 'failed' });
  }
});

/** Pull quotes in chunks (Tradier supports comma-separated symbols) */
async function getQuotesBatch(symbols) {
  const CHUNK = 140; // stay safe
  const out = [];
  for (let i=0; i<symbols.length; i+=CHUNK) {
    const slice = symbols.slice(i, i+CHUNK);
    const { data } = await axios.get(`${TRADIER_BASE}/markets/quotes`, {
      headers: H_JSON(), params: { symbols: slice.join(",") }
    });
    const q = data?.quotes?.quote;
    if (!q) continue;
    const arr = Array.isArray(q) ? q : [q];
    out.push(...arr);
  }
  return out;
}

/** Very light options scan: nearest expiration, ±20% moneyness; flag contracts where volume > OI and volume >= 500 */
async function scanOptionsUOA(root, { moneyness=0.2, minVol=500 } = {}) {
  try {
    const exps = await getExpirations(root);
    if (!exps?.length) return { count: 0, top: [] };
    const near = exps[0];
    const { data } = await axios.get(`${TRADIER_BASE}/markets/options/chains`, { headers: H_JSON(), params: { symbol: root, expiration: near } });
    const arr = data?.options?.option || [];
    // estimate underlying
    const und = await getUnderlyingLast(root);
    const within = (o) => und ? (Math.abs(o.strike - und) / und) <= moneyness : true;
    const flagged = arr.filter(o => within(o) && Number(o.volume || 0) >= minVol && Number(o.volume || 0) > Number(o.open_interest || 0))
                       .sort((a,b) => (b.volume||0) - (a.volume||0));
    return {
      count: flagged.length,
      top: flagged.slice(0, 3).map(o => ({
        occ: o.symbol, vol: Number(o.volume||0), oi: Number(o.open_interest||0), last: Number(o.last||0)
      }))
    };
  } catch {
    return { count: 0, top: [] };
  }
}

/** Classify by avg volume proxy (since market cap isn’t in v1 quotes) */
function bucketByAvgVol(avgVol) {
  if (avgVol >= 10_000_000) return "large";
  if (avgVol >= 1_000_000)  return "mid";
  return "small";
}

/** GET /scan
 *  Query:
 *    symbols   = CSV of roots (optional; defaults to POPULAR)
 *    limit     = top N per group (default 25)
 *    moneyness = options scan window (default 0.2 = ±20%)
 *    minVol    = min contract volume for UOA (default 500)
 */
app.get("/scan", async (req, res) => {
  try {
    const provider = String(req.query.provider || "alpaca").toLowerCase();
    const by = String(req.query.by || "volume"); // "volume" | "trade_count"
    const top = Number(req.query.top || 25);
    // const moneyness = Number(req.query.moneyness || 0.2);
    // const minVol = Number(req.query.minVol || 500);

    if (provider === "polygon") {
      // Gather
      const [act, mv] = await Promise.all([
        polyMostActives({ by, top }),
        polyTopMovers()
      ]);

      // UOA for top N by activity
      const roots = act.slice(0, Math.min(20, act.length)).map(x => x.symbol);
      const uoaMap = {};
      await Promise.all(roots.map(async (r) => { uoaMap[r] = await polyUOAForRoot(r, { moneyness, minVol }); }));

      const decorate = (arr) => arr.map(r => ({
        ...r,
        uoa_count: uoaMap[r.symbol]?.count ?? 0,
        uoa_top: uoaMap[r.symbol]?.top ?? []
      }));

      return res.json({
        ok: true,
        ts: Date.now(),
        provider: "polygon",
        groups: {
          most_actives: decorate(act),
          gainers: mv.gainers,
          losers: mv.losers,
        },
        params: { by, top, moneyness, minVol }
      });
    }
    
    const symbols = (String(req.query.symbols || "") || "").split(",").map(s => s.trim().toUpperCase()).filter(Boolean);
    //const roots = symbols.length ? symbols : await getPopularRoots();
    const { symbols: dynRoots } = await getPopularRoots();
    const roots = symbols.length ? symbols : dynRoots;

    const limit = Number(req.query.limit || 25);
    const moneyness = Number(req.query.moneyness || 0.2);
    const minVol = Number(req.query.minVol || 500);

    const quotes = await getQuotesBatch(roots);
    // normalize rows
    const rows = quotes.map(q => ({
      symbol: q.symbol,
      last: Number(q.last || q.close || 0),
      volume: Number(q.volume || 0),
      avg_volume: Number(q.average_volume || 0),
    })).map(r => ({ ...r, vr: r.avg_volume > 0 ? r.volume / r.avg_volume : 0 }));

    // group by size proxy
    const large = []; const mid = []; const small = [];
    for (const r of rows) {
      const b = bucketByAvgVol(r.avg_volume);
      if (b === "large") large.push(r);
      else if (b === "mid") mid.push(r);
      else small.push(r);
    }

    // rank by activity (volume ratio) within each bucket
    const rank = (arr) => arr.filter(r => Number.isFinite(r.vr)).sort((a,b) => (b.vr) - (a.vr)).slice(0, limit);

    const popular = [...rows].sort((a,b) => (b.avg_volume - a.avg_volume)).slice(0, limit);
    const most_active_large = rank(large);
    const most_active_mid   = rank(mid);
    const most_active_small = rank(small);

    // quick options UOA ping for the top 20 symbols overall by vr to avoid overload
    const uoaTargets = [...rows].sort((a,b)=>b.vr-a.vr).slice(0, Math.min(20, rows.length)).map(r=>r.symbol);
    const uoaMap = {};
    await Promise.all(uoaTargets.map(async (root) => {
      uoaMap[root] = await scanOptionsUOA(root, { moneyness, minVol });
    }));

    const decorate = (arr) => arr.map(r => ({
      ...r,
      uoa_count: uoaMap[r.symbol]?.count || 0,
      uoa_top: uoaMap[r.symbol]?.top || []
    }));

    res.json({
      ok: true,
      ts: Date.now(),
      groups: {
        popular: decorate(popular),
        most_active_large: decorate(most_active_large),
        most_active_mid: decorate(most_active_mid),
        most_active_small: decorate(most_active_small)
      },
      params: { roots, limit, moneyness, minVol }
    });
  } catch (e) {
    const detail = errToJson(e);
    console.error("/scan error:", detail);
    res.status(detail.status || 500).json({ ok:false, error: detail });
  }
});

/* ========================== ALPACA SCANNER (NEW) ========================== */

const {
  APCA_API_KEY_ID = "",
  APCA_API_SECRET_KEY = ""
} = process.env;

const ALPACA = axios.create({
  baseURL: "https://data.alpaca.markets",
  headers: {
    "Apca-Api-Key-Id": APCA_API_KEY_ID,
    "Apca-Api-Secret-Key": APCA_API_SECRET_KEY
  },
  timeout: 15_000
});

function applyAlpacaHeaders() {
  if (!ALPACA) return;
  if (CREDENTIALS.alpaca.key)    ALPACA.defaults.headers["Apca-Api-Key-Id"] = CREDENTIALS.alpaca.key;
  if (CREDENTIALS.alpaca.secret) ALPACA.defaults.headers["Apca-Api-Secret-Key"] = CREDENTIALS.alpaca.secret;
}
applyAlpacaHeaders();

// --- Axios outbound logging helper ---
function attachAxiosLogging(instance, label = "axios") {
  instance.interceptors.request.use((config) => {
    const store = reqStore.getStore();
    const rid = store?.rid || "n/a";
    const pvd = store?.provider || "n/a";
    const started = Date.now();
    // stash start time on the config for duration calc
    config.headers = config.headers || {};
    config.headers["x-request-id"] = rid; // propagate rid downstream
    config.metadata = { started };

    const url = `${config.baseURL || ""}${config.url || ""}`;
    console.log(
      `[${rid}] -> ${label} ${config.method?.toUpperCase()} ${url} params=${JSON.stringify(config.params || {})} provider=${pvd}`
    );
    return config;
  });

  instance.interceptors.response.use(
    (resp) => {
      const rid = reqStore.getStore()?.rid || "n/a";
      const dur = (resp.config.metadata?.started ? Date.now() - resp.config.metadata.started : 0);
      const url = `${resp.config.baseURL || ""}${resp.config.url || ""}`;
      console.log(
        `[${rid}] <- ${label} ${resp.status} ${url} (${dur} ms)`
      );
      return resp;
    },
    (err) => {
      const store = reqStore.getStore();
      const rid = store?.rid || "n/a";
      const dur = (err.config?.metadata?.started ? Date.now() - err.config.metadata.started : 0);
      const url = `${err.config?.baseURL || ""}${err.config?.url || ""}`;
      const status = err.response?.status || "ERR";
      const msg = err.response?.data?.message || err.message;
      console.warn(
        `[${rid}] <- ${label} ${status} ${url} (${dur} ms) error=${msg}`
      );
      return Promise.reject(err);
    }
  );
}

// Most Actives (by volume or trade_count)
async function alpacaMostActives({ by = "volume", top = 25 } = {}) {
  // GET /v1beta1/screener/stocks/most-actives?by=volume&top=25
  // Docs: https://docs.alpaca.markets/reference/mostactives-1
  const { data } = await ALPACA.get(
    "/v1beta1/screener/stocks/most-actives",
    { params: { by, top } }
  );
  // Expect: { most_actives: [{ symbol, volume, trade_count, ... }, ...] }
  return data?.most_actives ?? [];
}

// Top Movers: returns { gainers: [...], losers: [...] }
async function alpacaMovers({ top = 25 } = {}) {
  // GET /v1beta1/screener/stocks/movers  (market_type is in the PATH)
  // Docs: https://docs.alpaca.markets/reference/movers-1
  const { data } = await ALPACA.get("/v1beta1/screener/stocks/movers", {
    params: { top }
  });
  // shape: { gainers: [...], losers: [...] }
  return {
    gainers: Array.isArray(data?.gainers) ? data.gainers : [],
    losers: Array.isArray(data?.losers) ? data.losers : []
  };
}

// Snapshots for today vs previous day volumes
async function alpacaSnapshots(symbols = []) {
  // GET /v2/stocks/snapshots?symbols=...
  // Docs: https://docs.alpaca.markets/reference/stocksnapshots-1
  const MAX = 50; // safe batch size
  const out = {};
  for (let i = 0; i < symbols.length; i += MAX) {
    const slice = symbols.slice(i, i + MAX);
    const { data } = await ALPACA.get("/v2/stocks/snapshots", {
      params: { symbols: slice.join(",") }
    });
    // shape: { snapshots: { SYM: { dailyBar, prevDailyBar, ... } , ... } }
    const snaps = data?.snapshots || {};
    Object.assign(out, snaps);
  }
  return out;
}

function num(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

// Enrich rows with volume ratio vs yesterday using snapshots
function enrichWithPrevVol(rows = [], snapMap = {}) {
  return rows.map((r) => {
    const sym = r.symbol || r.S;
    const snap = snapMap[sym] || {};
    const dbar = snap.dailyBar || snap.daily_bar || {};
    const pbar = snap.prevDailyBar || snap.prev_daily_bar || {};
    const volToday = num(dbar.v ?? dbar.volume);
    const volPrev  = num(pbar.v ?? pbar.volume);
    const vr_prev  = volPrev > 0 ? volToday / volPrev : 0;
    return { ...r, vol_today: volToday, vol_prev: volPrev, vr_prev };
  });
}

// -------- 7-day rolling history (in-memory) --------
const ALP_HISTORY = []; // [{ ts, most_actives_by, most_actives, gainers, losers }]
function pushHistory(snap) {
  const now = Date.now();
  const sevenDays = 7 * 24 * 60 * 60 * 1000;
  ALP_HISTORY.push({ ts: now, ...snap });
  // prune
  while (ALP_HISTORY.length && (now - ALP_HISTORY[0].ts) > sevenDays) ALP_HISTORY.shift();
}
// ---- Fetch recent daily bars for ATR ----
async function alpacaDailyBars(symbols = [], limit = 20) {
  // GET /v2/stocks/bars?timeframe=1Day&symbols=...&limit=20
  const MAX = 50;
  const out = {};
  for (let i = 0; i < symbols.length; i += MAX) {
    const slice = symbols.slice(i, i + MAX);
    const { data } = await ALPACA.get("/v2/stocks/bars", {
      params: { symbols: slice.join(","), timeframe: "1Day", limit }
    });
    // shape: { bars: { SYM: [ { o,h,l,c,v,t }, ... ] } }
    const bars = data?.bars || {};
    Object.assign(out, bars);
  }
  return out; // { SYM: [bars...] }
}

function computeATR14(bars = []) {
  // bars: most-recent last; we’ll work from oldest->newest
  if (!Array.isArray(bars) || bars.length < 15) return 0;
  const arr = [...bars].slice(-15); // need 15 to get 14 TRs
  // ensure ascending by time:
  arr.sort((a,b) => new Date(a.t).valueOf() - new Date(b.t).valueOf());

  let prevClose = Number(arr[0].c);
  let sumTR = 0;
  for (let i = 1; i < arr.length; i++) {
    const b = arr[i];
    const h = Number(b.h), l = Number(b.l), cPrev = Number(prevClose);
    const tr = Math.max(h - l, Math.abs(h - cPrev), Math.abs(l - cPrev));
    sumTR += tr;
    prevClose = Number(b.c);
  }
  return sumTR / 14;
}

// Decide the “last price” based on session
function chooseSessionPrice(snapshot, session) {
  // snapshot: { latestTrade, dailyBar, prevDailyBar }
  const latest = Number(snapshot?.latestTrade?.p ?? snapshot?.latest_trade?.p ?? NaN);
  const dbar = snapshot?.dailyBar || snapshot?.daily_bar || {};
  const pbar = snapshot?.prevDailyBar || snapshot?.prev_daily_bar || {};
  const closeToday = Number(dbar?.c ?? NaN);
  const openToday  = Number(dbar?.o ?? NaN);
  const prevClose  = Number(pbar?.c ?? NaN);

  const isNum = (x) => Number.isFinite(x);

  if (session === "pre" || session === "post") {
    // Use latest trade vs prev close as “session” reference
    if (isNum(latest)) return { price: latest, prevClose, openToday };
  }

  // Regular session default: use today’s close if present, else fall back to latest
  if (isNum(closeToday)) return { price: closeToday, prevClose, openToday };
  if (isNum(latest))     return { price: latest,     prevClose, openToday };
  return { price: NaN, prevClose, openToday };
}

// % helpers
const pct = (num, den) => (Number.isFinite(num) && Number.isFinite(den) && den !== 0) ? (num/den - 1) : 0;

// GET /alpaca/scan
// Query: by=volume|trade_count  top=25  refresh=1|0 (default 1)
// app.get("/alpaca/scan", async (req, res) => {
//   try {
//     const by = String(req.query.by || "volume");
//     const top = Number(req.query.top || 30);
//     const refresh = req.query.refresh === "0" ? 0 : 1;

//     if (!APCA_API_KEY_ID || !APCA_API_SECRET_KEY) {
//       return res.status(400).json({ ok: false, error: "Missing APCA_API_KEY_ID / APCA_API_SECRET_KEY" });
//     }

//     if (!CREDENTIALS.alpaca.key || !CREDENTIALS.alpaca.secret) {
//       return res.status(400).json({ ok:false, error:"Missing Alpaca credentials" });
//     }

//     let payload;
//     if (refresh) {
//       // // Pull most-actives + movers
//       // const [actives, gainers, losers] = await Promise.all([
//       //   alpacaMostActives({ by, top }),
//       //   alpacaMovers({ direction: "gainers", top }),
//       //   alpacaMovers({ direction: "losers", top })
//       // ]);

//       // Pull most-actives + movers (both lists in one call)
//       const [actives, mv] = await Promise.all([
//         alpacaMostActives({ by, top }),
//         alpacaMovers({ top })
//       ]);
//       const gainers = mv.gainers;
//       const losers  = mv.losers;

//       // Enrich with prev-day volume ratio (for a 'high vol' feel)
//       const wanted = [
//         ...new Set([
//           ...actives.map(x => x.symbol),
//           ...gainers.map(x => x.symbol),
//           ...losers.map(x => x.symbol),
//         ])
//       ];
//       const snaps = wanted.length ? await alpacaSnapshots(wanted) : {};
//       const activesE = enrichWithPrevVol(actives, snaps);
//       const gainersE = enrichWithPrevVol(gainers, snaps);
//       const losersE  = enrichWithPrevVol(losers,  snaps);

//       payload = {
//         ok: true,
//         ts: Date.now(),
//         params: { by, top },
//         groups: {
//           most_actives: activesE,
//           gainers: gainersE,
//           losers: losersE
//         }
//       };
//       //pushHistory({ most_actives_by: by, most_actives: activesE, gainers: gainersE, losers: losersE });
//       pushHistory({ most_actives_by: by, most_actives: activesE, gainers: gainersE, losers: losersE });
//     } else {
//       // serve cached latest
//       const latest = ALP_HISTORY[ALP_HISTORY.length - 1];
//       payload = latest ? { ok: true, ts: latest.ts, params: { by: latest.most_actives_by, top }, groups: {
//         most_actives: latest.most_actives,
//         gainers: latest.gainers,
//         losers: latest.losers
//       }} : { ok:false, error:"no cached scan yet" };
//     }

//     // Also include a light history summary (symbol hit counts over last 7d)
//     const hitCount = {};
//     for (const snap of ALP_HISTORY) {
//       for (const r of (snap.most_actives || [])) hitCount[r.symbol] = (hitCount[r.symbol] || 0) + 1;
//       for (const r of (snap.gainers || []))      hitCount[r.symbol] = (hitCount[r.symbol] || 0) + 1;
//       for (const r of (snap.losers || []))       hitCount[r.symbol] = (hitCount[r.symbol] || 0) + 1;
//     }
//     payload.history = {
//       days: 7,
//       samples: ALP_HISTORY.length,
//       top_hits: Object.entries(hitCount)
//         .sort((a,b)=>b[1]-a[1])
//         .slice(0, 50)
//         .map(([symbol, hits]) => ({ symbol, hits }))
//     };

//     res.json(payload);
//   } catch (e) {
//     const detail = e?.response ? { status: e.response.status, data: e.response.data } : { message: String(e) };
//     console.error("/alpaca/scan error:", detail);
//     res.status(detail.status || 500).json({ ok:false, error: detail });
//   }
// });
app.get("/alpaca/scan", async (req, res) => {
  try {
    const by      = String(req.query.by || "volume");      // "volume" | "trade_count"
    const top     = Number(req.query.top || 30);
    const refresh = req.query.refresh === "0" ? 0 : 1;

    // NEW: filters
    let session = String(req.query.session || "regular").toLowerCase(); // "pre" | "regular" | "post"
    if (!["pre","regular","post"].includes(session)) session = "regular";

    let filter = String(req.query.filter || "").toLowerCase(); // "gapup" | "gapdown" | ""
    if (!["gapup","gapdown",""].includes(filter)) filter = "";

    const minGap = Number.isFinite(+req.query.minGap) ? Math.max(0, Number(req.query.minGap)) : 0.02; // 2%

    if (!APCA_API_KEY_ID || !APCA_API_SECRET_KEY) {
      return res.status(400).json({ ok: false, error: "Missing APCA_API_KEY_ID / APCA_API_SECRET_KEY" });
    }
    if (!CREDENTIALS.alpaca.key || !CREDENTIALS.alpaca.secret) {
      return res.status(400).json({ ok:false, error:"Missing Alpaca credentials" });
    }

    // Pull most-actives & movers (you already had this)
    const [actives, mv] = await Promise.all([
      alpacaMostActives({ by, top }),
      alpacaMovers({ top })
    ]);
    const gainers = mv.gainers || [];
    const losers  = mv.losers  || [];

    // Build symbol universe to enrich
    const wanted = [
      ...new Set([
        ...actives.map(x => x.symbol),
        ...gainers.map(x => x.symbol),
        ...losers.map(x => x.symbol),
      ])
    ];

    // Snapshots for price/prevClose/open; Daily bars for ATR
    const [snaps, barsMap] = await Promise.all([
      alpacaSnapshots(wanted),
      alpacaDailyBars(wanted, 20)
    ]);

    // Decorate with session-aware price, %change, gap %, ATR
    const decorate = (rows) => rows.map(r => {
      const sym = r.symbol || r.S;
      const snap = snaps[sym] || {};
      const { price, prevClose, openToday } = chooseSessionPrice(snap, session);
      const isNum = (x) => Number.isFinite(x);

      const change_pct = (isNum(price) && isNum(prevClose)) ? (price / prevClose - 1) : 0;
      const gap_pct    = (isNum(openToday) && isNum(prevClose)) ? (openToday / prevClose - 1) : 0;

      const atr14 = computeATR14(barsMap[sym] || []);

      return {
        ...r,
        symbol: sym,
        price,
        last: price,
        change_pct,  // e.g., 0.034 = +3.4%
        gap_pct,     // open vs prev close
        atr14,       // in price units
      };
    });

    let most = decorate(actives);
    let g    = decorate(gainers);
    let l    = decorate(losers);

    // Apply gap filter if requested
    if (filter === "gapup") {
      const pred = (row) => Number.isFinite(row.gap_pct) && row.gap_pct >= minGap;
      most = most.filter(pred);
      g    = g.filter(pred);
      l    = l.filter(pred);
    } else if (filter === "gapdown") {
      const pred = (row) => Number.isFinite(row.gap_pct) && row.gap_pct <= -minGap;
      most = most.filter(pred);
      g    = g.filter(pred);
      l    = l.filter(pred);
    }

    return res.json({
      ok: true,
      ts: Date.now(),
      params: { by, top, session, filter, minGap },
      groups: {
        most_actives: most,
        gainers: g,
        losers: l
      }
    });
  } catch (e) {
    const detail = e?.response ? { status: e.response.status, data: e.response.data } : { message: String(e) };
    console.error("/alpaca/scan error:", detail);
    res.status(detail.status || 500).json({ ok:false, error: detail });
  }
});

// ======= COMBINED POPULAR (Alpaca by volume ∪ curated fallback) =======
async function getPopularByVolume(top = 40) {
  // uses your existing ALPACA axios instance
  const resp = await ALPACA.get('/v1beta1/screener/stocks/most-actives', {
    params: { by: 'volume', top: String(top) },
    validateStatus: () => true,
  });
  if (resp.status >= 400 || resp.data?.message) {
    const msg = resp.data?.message || `HTTP ${resp.status}`;
    throw new Error(`alpaca screener error: ${msg}`);
  }
  const rows = resp.data?.most_actives || [];
  return Array.from(new Set(rows.map(r => r.symbol).filter(Boolean)));
}

function uniqueMerge(a = [], b = []) {
  const s = new Set(a);
  for (const x of b) s.add(x);
  return [...s];
}

// GET /popular/combined?top=40
app.get('/popular/combined', async (req, res) => {
  try {
    const top = Math.max(1, Math.min(100, Number(req.query.top || 40)));
    // live list by volume + curated fallback (POPULAR_FALLBACK already in your file)
    let alp = [];
    try { alp = await getPopularByVolume(top); } catch (e) { /* fall back silently */ }
    const combined = uniqueMerge(alp, POPULAR_FALLBACK).slice(0, top);
    res.json({ ok: true, ts: Date.now(), symbols: combined, source: alp.length ? 'alpaca+fallback' : 'fallback-only' });
  } catch (e) {
    res.status(500).json({ ok:false, error: e?.message || 'failed' });
  }
});

/* ======================== END ALPACA SCANNER (NEW) ======================== */
// POST /settings/credentials
// body: { tradier?: { token }, alpaca?: { key, secret } }
app.post("/settings/credentials", async (req, res) => {
  try {
    const { tradier, alpaca } = req.body || {};

    if (tradier?.token) {
      CREDENTIALS.tradier.token = String(tradier.token).trim();
    }
    if (alpaca?.key)    CREDENTIALS.alpaca.key    = String(alpaca.key).trim();
    if (alpaca?.secret) CREDENTIALS.alpaca.secret = String(alpaca.secret).trim();
    applyAlpacaHeaders();

    // Optional shallow validation (won't log secrets)
    const tests = [];
    if (tradier?.token) {
      tests.push(
        axios.get(`${TRADIER_BASE}/markets/quotes`, {
          headers: H_JSON(),
          params: { symbols: "SPY" },
          validateStatus: () => true
        }).then(r => ({ provider:"tradier", status:r.status }))
         .catch(() => ({ provider:"tradier", status:0 }))
      );
    }
    if (alpaca?.key || alpaca?.secret) {
      tests.push(
        ALPACA.get("/v1beta1/screener/stocks/most-actives", {
          params: { by:"volume", top:1 },
          validateStatus: () => true
        }).then(r => ({ provider:"alpaca", status:r.status }))
         .catch(() => ({ provider:"alpaca", status:0 }))
      );
    }
    const results = await Promise.all(tests);

    const test = {};
    for (const r of results) {
      test[r.provider] = { ok: r.status && r.status < 400 };
    }

    // Don’t restart streams automatically here — the client will call /watch right after save.
    res.json({ ok:true, test });
  } catch (e) {
    res.status(500).json({ ok:false, error: errToJson(e) });
  }
});
/* ------------------------ boot ------------------------ */
server.listen(Number(PORT1), () => {
  console.log(`HTTP on ${PORT1} | WS on ${WS_PORT}`);
  console.log(`Try: curl "http://localhost:${PORT}/scan?limit=15"`);
  console.log(`Try: curl "http://localhost:${PORT}/watch?symbols=NVDA&eqForTS=NVDA&backfill=10"`);
});

