// server.js (ESM)

// ===== Imports =====
import "dotenv/config";
import express from "express";
import cors from "cors";
import morgan from "morgan";
import { v4 as uuidv4 } from "uuid";
import { AsyncLocalStorage } from "node:async_hooks";

import { WebSocketServer, WebSocket } from "ws";
import { parse as parseUrl } from "url";
import http from "http";

import axios from "axios";
import * as ES from "eventsource"; const EventSource = ES.default ?? ES;
import { DateTime } from "luxon";

// Your modules
import { POLY, startPolygonWatch } from "./polygon.js";

// ===== Env / Constants =====
const {
  TRADIER_BASE = "",
  TRADIER_TOKEN = "",
} = process.env;

const PORT = Number(process.env.PORT || 3000);
if (!TRADIER_BASE || !TRADIER_TOKEN) throw new Error("Missing TRADIER_* envs");

// ===== AsyncLocalStorage =====
export const reqStore = new AsyncLocalStorage();

// ===== App / Middleware =====
const app = express();
app.use(cors({ origin: "*", credentials: true }));

// CORS & headers
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "*");
  next();
});

app.use(express.json({ limit: "64kb" }));

// Request id + ALS
app.use((req, res, next) => {
  const rid = req.headers["x-request-id"]?.toString() || uuidv4();
  req.id = rid;
  const provider = (req.query?.provider || "").toString().toLowerCase() || "n/a";

  reqStore.run({ rid, provider, started: Date.now() }, () => {
    res.setHeader("x-request-id", rid);
    next();
  });
});

// Logging
morgan.token("rid", (req) => req.id);
morgan.token("provider", (req) => (req.query?.provider || "n/a").toString());
app.use(morgan('[:rid] :method :url :status :res[content-length] - :response-time ms (provider=:provider)'));

// ===== Axios logging helper =====
attachAxiosLogging(axios, "tradier");
// attachAxiosLogging(ALPACA, "alpaca"); // if/when used
// attachAxiosLogging(POLY, "polygon");  // if you want polygon logs

// ===== HTTP server (single port) =====
const server = http.createServer(app);

// ===== WebSocket (external https server pattern) =====
const wss = new WebSocketServer({
  noServer: true,
  clientTracking: true,
  perMessageDeflate: false, // friendlier behind proxies
});

// Optional: auth/verify upgrade
function verifyClient(_req) {
  return true; // plug in header/token checks if needed
}

server.on("upgrade", (req, socket, head) => {
  const { pathname } = parseUrl(req.url || "/");
  if (pathname !== "/ws") {
    socket.destroy();
    return;
  }
  if (!verifyClient(req)) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req);
  });
});

// Per-connection setup
function attachClientState(ws) {
  ws.subs = new Set();
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));
}

wss.on("connection", (ws) => {
  attachClientState(ws);

  // simple protocol for subs (optional)
  ws.on("message", (buf) => {
    let msg; try { msg = JSON.parse(buf.toString()); } catch { return; }
    if (msg?.type === "sub" && Array.isArray(msg.symbols)) {
      msg.symbols.forEach(s => ws.subs.add(String(s).toUpperCase()));
      safeSend(ws, { type: "sub_ok", symbols: [...ws.subs] });
    } else if (msg?.type === "unsub" && Array.isArray(msg.symbols)) {
      msg.symbols.forEach(s => ws.subs.delete(String(s).toUpperCase()));
      safeSend(ws, { type: "unsub_ok", symbols: [...ws.subs] });
    } else if (msg?.type === "ping") {
      safeSend(ws, { type: "pong", t: Date.now() });
    }
  });

  safeSend(ws, { type: "hello", t: Date.now() });
});

// Keepalive for proxies
const KA = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { try { ws.terminate(); } catch {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 30_000);

wss.on("close", () => clearInterval(KA));

// ===== Broadcast helpers =====
function safeSend(ws, obj) {
  if (ws.readyState !== WebSocket.OPEN) return false;
  if (ws.bufferedAmount > 1_000_000) return false;
  try { ws.send(JSON.stringify(obj)); return true; }
  catch { try { ws.terminate(); } catch {} return false; }
}

const broadcast = (msg) => {
  for (const c of wss.clients) safeSend(c, msg);
};

const broadcastSymbol = (symbol, msg) => {
  for (const c of wss.clients) {
    if (c.subs?.has(symbol)) safeSend(c, msg);
  }
};

// ====== Existing logic (unchanged, but uses `broadcast`) ======

// dynamic headers for Tradier
const CREDENTIALS = {
  tradier: { token: process.env.TRADIER_TOKEN || "" },
  alpaca:  { key: process.env.APCA_API_KEY_ID || "", secret: process.env.APCA_API_SECRET_KEY || "" },
};
function H_JSON() { return { Authorization: `Bearer ${CREDENTIALS.tradier.token}`, Accept: "application/json" }; }
function H_SSE()  { return { Authorization: `Bearer ${CREDENTIALS.tradier.token}`, Accept: "text/event-stream" }; }

const isSandbox = TRADIER_BASE.includes("sandbox");
const TS_INTERVAL = isSandbox ? "1min" : "tick";
const errToJson = (e) => (e?.response ? { status: e.response.status, data: e.response.data } : { message: String(e) });

function parseDay(req) {
  const d = String(req.query.day || "").trim();
  if (d && /^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
  const nowNY = DateTime.now().setZone("America/New_York");
  if (nowNY.weekday === 6) return nowNY.minus({ days: 1 }).toFormat("yyyy-LL-dd"); // Sat->Fri
  if (nowNY.weekday === 7) return nowNY.minus({ days: 2 }).toFormat("yyyy-LL-dd"); // Sun->Fri
  return nowNY.toFormat("yyyy-LL-dd");
}

/* -------------------- your existing state/helpers -------------------- */
const midBySym = new Map();
const lastTradeBySym = new Map();
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

/* -------------------- options helpers (unchanged) -------------------- */
const oiByOcc = new Map();
const volByOcc = new Map();
const TH_CLOSE_BY_DVOL = 0.8;
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

/* -------------------- polygon helpers (unchanged) -------------------- */
async function polyTopMovers() {
  const [g, l] = await Promise.all([
    POLY.get("/v2/snapshot/locale/us/markets/stocks/gainers"),
    POLY.get("/v2/snapshot/locale/us/markets/stocks/losers"),
  ]);
  return {
    gainers: Array.isArray(g.data?.tickers) ? g.data.tickers : [],
    losers:  Array.isArray(l.data?.tickers) ? l.data.tickers : [],
  };
}
async function polyMostActives({ by = "volume", top = 30 } = {}) {
  const r = await POLY.get("/v2/snapshot/locale/us/markets/stocks/tickers");
  const rows = Array.isArray(r.data?.tickers) ? r.data.tickers : [];
  const norm = rows.map(t => {
    const vol = Number(t.day?.v ?? t.day?.volume ?? 0);
    const tradeCount = Number(t.day?.n ?? t.day?.transactions ?? 0);
    const last = Number(t.lastTrade?.p ?? 0);
    return {
      symbol: t.ticker,
      volume: vol,
      trade_count: tradeCount,
      last,
      change_percent: Number(t.todaysChangePerc ?? 0) / 100,
    };
  });
  const key = by === "trade_count" ? "trade_count" : "volume";
  return norm.sort((a,b)=> (b[key]||0)-(a[key]||0)).slice(0, top);
}

/* -------------------- backfill (unchanged) -------------------- */
async function backfillEquityTS(symbol, day, minutes = 5) {
  try {
    const start = DateTime.fromISO(`${day}T09:30:00`, { zone: "America/New_York" });
    const end = start.plus({ minutes });
    const params = {
      symbol, interval: isSandbox ? "1min" : "tick",
      start: start.toFormat("yyyy-LL-dd HH:mm:ss"),
      end: end.toFormat("yyyy-LL-dd HH:mm:ss"),
    };
    const { data } = await axios.get(`${TRADIER_BASE}/markets/timesales`, { headers: H_JSON(), params });
    (data?.series?.data ?? []).forEach(tick => {
      const price = Number(tick.price ?? tick.last ?? tick.close ?? 0);
      const { side, side_src } = inferSideServer(symbol, price);
      lastTradeBySym.set(symbol, price);

      const book = midBySym.get(symbol) || {};
      const eps = epsFor(book);
      let at = "between";
      if (book.ask && price >= book.ask - eps) at = "ask";
      else if (book.bid && price <= book.bid + eps) at = "bid";
      else if (book.mid && Math.abs(price - book.mid) <= eps) at = "mid";

      broadcast({ type: "equity_ts", symbol, data: { ...tick, side, side_src, at, book } });
    });
  } catch (e) {
    console.error("backfillEquityTS error:", errToJson(e));
  }
}

/* -------------------- streaming quotes (unchanged) -------------------- */
let esQuotes = null;
function startQuoteStream(symbolsOrOCC=[]) {
  try {
    if (esQuotes) esQuotes.close();
    const uniq = [...new Set(symbolsOrOCC)].filter(Boolean);
    if (uniq.length === 0) return;
    const url = `${TRADIER_BASE}/markets/events?symbols=${encodeURIComponent(uniq.join(","))}&sessionid=${Date.now()}`;
    const es  = new EventSource(url, { headers: H_SSE() }); esQuotes = es;

    es.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg?.type === "quote" && msg?.data) {
          const q = msg.data;
          updateMidFromQuote(q);

          if (q.last && q.size && q.size > 0) {
            const rootSym = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(q.symbol)
              ? q.symbol.replace(/\d{6}[CP]\d{8}$/, "")
              : q.symbol;

            const price = Number(q.last);
            const { side, side_src } = inferSideServer(rootSym, price);
            lastTradeBySym.set(rootSym, price);
            broadcast({ type: "quotes", data: q, side, side_src });

            const isOCC = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(q.symbol);
            if (isOCC) {
              const optSide = inferSideServer(q.symbol, price);
              const book = midBySym.get(q.symbol) || {};
              const eps = epsFor(book);
              let at = "between";
              if (book.ask && price >= book.ask - eps) at = "ask";
              else if (book.bid && price <= book.bid + eps) at = "bid";
              else if (book.mid && Math.abs(price - book.mid) <= eps) at = "mid";

              const seenVol = Number(q.volume ?? NaN);
              let priorVol = volByOcc.has(q.symbol) ? volByOcc.get(q.symbol) : 0;
              if (Number.isFinite(seenVol)) priorVol = Math.max(0, seenVol - Number(q.size));
              if (Number.isFinite(seenVol)) volByOcc.set(q.symbol, seenVol);
              else volByOcc.set(q.symbol, (volByOcc.get(q.symbol) || 0) + Number(q.size));
              const oi = oiByOcc.get(q.symbol) ?? null;

              const { action, action_conf } = classifyOpenClose({
                qty: Number(q.size), oi, priorVol, side: optSide.side, at
              });

              broadcast({
                type: "option_ts",
                symbol: q.symbol,
                data: {
                  id: `ots_${q.symbol}_${q.trade_time || Date.now()}`,
                  ts: Date.now(),
                  option: { expiry: "", strike: 0, right: /C\d{8}$/i.test(q.symbol) ? "C" : "P" },
                  qty: q.size,
                  price,
                  side: optSide.side,
                  side_src: optSide.side_src,
                  oi, priorVol, book, at,
                  action, action_conf
                }
              });
            }
          } else {
            broadcast({ type: "quotes", data: q });
          }
        }
      } catch { /* ignore */ }
    };
    es.onerror = () => { es.close(); setTimeout(() => startQuoteStream(uniq), 1500); };
  } catch (e) {
    console.error("startQuoteStream error:", errToJson(e));
  }
}

// ===== Routes (unchanged except using helpers above) =====
app.get("/health", (_req, res) => res.status(200).send("OK"));

let stopCurrentWatch = null;
app.get("/watch", async (req, res) => {
  try {
    const provider = String(req.query.provider || "tradier").toLowerCase();
    const syms = String(req.query.symbols || "SPY").split(",").map(s=>s.trim().toUpperCase()).filter(Boolean);
    const eqForTS = String(req.query.eqForTS || syms.join(",")).split(",").map(s=>s.trim().toUpperCase()).filter(Boolean);
    const backfillMins = req.query.backfill ? Number(req.query.backfill) : 5;
    const day = parseDay(req);
    const moneyness = req.query.moneyness ? Number(req.query.moneyness) : 0.25;
    const limit = req.query.limit ? Number(req.query.limit) : 150;
    const expiries = String(req.query.expiries || "").split(",").map(s=>s.trim()).filter(Boolean);

    if (typeof stopCurrentWatch === "function") { try { stopCurrentWatch(); } catch {} stopCurrentWatch = null; }

    console.log(`[watch] provider=${provider} syms=${syms.join(",")} eqForTS=${eqForTS.join(",")} backfill=${backfillMins} mny=${moneyness} limit=${limit}`);

    if (provider === "polygon") {
      stopCurrentWatch = await startPolygonWatch({
        equities: syms,
        moneyness,
        limit,
        broadcast, // uses external WS pattern safely
        classifyOptionAction: classifyOpenClose,
      });
      return res.json({
        ok: true,
        provider: "polygon",
        env: { provider: "polygon", base: "https://api.polygon.io", delayed: true },
        watching: { equities: syms, eqForTS, day, backfillMins, limit, moneyness, options_count: 0 }
      });
    }

    // ---- TRADIER ----
    const occSet = new Set();
    for (const root of syms) {
      try {
        const occ = await buildOptionsWatchList(
          root,
          { expiries, moneyness, limit: Math.ceil(limit / Math.max(1, syms.length)) }
        );
        occ.forEach(x => occSet.add(x));
      } catch (e) {
        console.warn("buildOptionsWatchList failed", root, errToJson(e));
      }
    }

    startQuoteStream([...syms, ...occSet]);
    if (backfillMins > 0) {
      await Promise.all(eqForTS.map(sym => backfillEquityTS(sym, day, backfillMins)));
    }

    return res.json({
      ok: true,
      provider: "tradier",
      env: { provider: "tradier", base: TRADIER_BASE, sandbox: isSandbox, ts_interval: TS_INTERVAL },
      watching: { equities: syms, options_count: occSet.size, eqForTS, day, backfillMins, limit, moneyness }
    });
  } catch (e) {
    const detail = errToJson(e);
    console.error("/watch error:", detail);
    res.status(detail.status || 500).json({ ok:false, error: detail });
  }
});

// ... keep your /popular, /scan, /alpaca/scan, /popular/combined, /settings/credentials routes here unchanged ...

// ===== Boot =====
server.listen(PORT, () => {
  console.log(`[http] listening on ${PORT}`);
  console.log(`WS upgrades on /ws`);
});

// ===== Helpers =====
function attachAxiosLogging(instance, label = "axios") {
  instance.interceptors.request.use((config) => {
    const store = reqStore.getStore();
    const rid = store?.rid || "n/a";
    const pvd = store?.provider || "n/a";
    const started = Date.now();
    config.headers = config.headers || {};
    config.headers["x-request-id"] = rid;
    config.metadata = { started };

    const url = `${config.baseURL || ""}${config.url || ""}`;
    console.log(`[${rid}] -> ${label} ${config.method?.toUpperCase()} ${url} params=${JSON.stringify(config.params || {})} provider=${pvd}`);
    return config;
  });

  instance.interceptors.response.use(
    (resp) => {
      const rid = reqStore.getStore()?.rid || "n/a";
      const dur = (resp.config.metadata?.started ? Date.now() - resp.config.metadata.started : 0);
      const url = `${resp.config.baseURL || ""}${resp.config.url || ""}`;
      console.log(`[${rid}] <- ${label} ${resp.status} ${url} (${dur} ms)`);
      return resp;
    },
    (err) => {
      const rid = reqStore.getStore()?.rid || "n/a";
      const dur = (err.config?.metadata?.started ? Date.now() - err.config.metadata.started : 0);
      const url = `${err.config?.baseURL || ""}${err.config?.url || ""}`;
      const status = err.response?.status || "ERR";
      const msg = err.response?.data?.message || err.message;
      console.warn(`[${rid}] <- ${label} ${status} ${url} (${dur} ms) error=${msg}`);
      return Promise.reject(err);
    }
  );
}

// ===== Option watchlist helpers you already have =====
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
