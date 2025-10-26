import "dotenv/config";
import express from "express";
import cors from "cors";
import { WebSocketServer } from "ws";
import * as ES from "eventsource"; const EventSource = ES.default ?? ES;
import axios from "axios";
import { DateTime } from "luxon";
// near top with other imports
import { startPolygonWatch } from "./polygon.js";

// --- Logging bootstrap ---
import morgan from "morgan";
import { v4 as uuidv4 } from "uuid";
import { AsyncLocalStorage } from "node:async_hooks";


attachAxiosLogging(axios, "tradier");      // your default axios (Tradier usage)
//attachAxiosLogging(ALPACA, "alpaca");      // your Alpaca axios instance
//attachAxiosLogging(POLY, "polygon");       // your Polygon axios instance

export const reqStore = new AsyncLocalStorage();

const app = express();
app.use(cors());

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

const { TRADIER_BASE="", TRADIER_TOKEN="", PORT=8080, WS_PORT=8081 } = process.env;
if (!TRADIER_BASE || !TRADIER_TOKEN) throw new Error("Missing TRADIER_* envs");

//const app = express();
//app.use(cors());

const wss = new WebSocketServer({ port: Number(WS_PORT) });
const broadcast = (msg) => {
  const s = JSON.stringify(msg);
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
};

const H_JSON = { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: "application/json" };
const H_SSE  = { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: "text/event-stream" };
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

/* ------------------------ equity backfill (existing) ------------------------ */
async function backfillEquityTS(symbol, day, minutes = 5) {
  try {
    const start = DateTime.fromISO(`${day}T09:30:00`, { zone: "America/New_York" });
    const end = start.plus({ minutes });
    const params = {
      symbol, interval: TS_INTERVAL,
      start: start.toFormat("yyyy-LL-dd HH:mm:ss"),
      end: end.toFormat("yyyy-LL-dd HH:mm:ss"),
    };
    const { data } = await axios.get(`${TRADIER_BASE}/markets/timesales`, { headers: H_JSON, params });
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

/* ------------------------ options helpers (existing) ------------------------ */
async function getExpirations(symbol) {
  const { data } = await axios.get(`${TRADIER_BASE}/markets/options/expirations`, { headers: H_JSON, params: { symbol } });
  return data?.expirations?.date ?? [];
}
async function getUnderlyingLast(symbol) {
  const { data } = await axios.get(`${TRADIER_BASE}/markets/quotes`, { headers: H_JSON, params: { symbols: symbol } });
  const q = data?.quotes?.quote;
  return Array.isArray(q) ? (+q[0]?.last || +q[0]?.close || 0) : (+q?.last || +q?.close || 0);
}
async function buildOptionsWatchList(symbol, { expiries=[], moneyness=0.25, limit=150 }={}) {
  const out = [];
  const und = await getUnderlyingLast(symbol);
  let exps = expiries.length ? expiries : (await getExpirations(symbol)).slice(0,1);
  for (const exp of exps) {
    const { data } = await axios.get(`${TRADIER_BASE}/markets/options/chains`, { headers: H_JSON, params: { symbol, expiration: exp } });
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

/* ------------------------ streaming quotes (existing) ------------------------ */
let esQuotes = null;
function startQuoteStream(symbolsOrOCC=[]) {
  try {
    if (esQuotes) esQuotes.close();
    const uniq = [...new Set(symbolsOrOCC)].filter(Boolean);
    if (uniq.length === 0) return;
    const url = `${TRADIER_BASE}/markets/events?symbols=${encodeURIComponent(uniq.join(","))}&sessionid=${Date.now()}`;
    const es  = new EventSource(url, { headers: H_SSE }); esQuotes = es;

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


let stopCurrentWatch = null;

app.get("/watch", async (req, res) => {
  try {
    const provider = String(req.query.provider || "tradier").toLowerCase();

    // Use syms (not "symbols") to avoid clashes anywhere else
    const syms = String(req.query.symbols || "SPY")
      .split(",")
      .map(s => s.trim().toUpperCase())
      .filter(Boolean);

    const eqForTS = String(req.query.eqForTS || syms.join(","))
      .split(",")
      .map(s => s.trim().toUpperCase())
      .filter(Boolean);

    const backfillMins = req.query.backfill ? Number(req.query.backfill) : 5;
    const day          = parseDay(req);

    const moneyness = req.query.moneyness ? Number(req.query.moneyness) : 0.25;
    const limit     = req.query.limit ? Number(req.query.limit) : 150;
    const expiries  = String(req.query.expiries || "")
      .split(",")
      .map(s => s.trim())
      .filter(Boolean);

    // cancel any previous loops
    if (typeof stopCurrentWatch === "function") {
      try { stopCurrentWatch(); } catch {}
      stopCurrentWatch = null;
    }

    console.log(`[watch] provider=${provider} syms=${syms.join(",")} eqForTS=${eqForTS.join(",")} backfill=${backfillMins} mny=${moneyness} limit=${limit}`);

    if (provider === "polygon") {
      // start polygon polling loops
      stopCurrentWatch = await startPolygonWatch({
        equities: syms,
        moneyness,
        limit,
        broadcast,
        classifyOptionAction: classifyOpenClose,
      });

      return res.json({
        ok: true,
        provider: "polygon",
        env: { provider: "polygon", base: "https://api.polygon.io", delayed: true },
        watching: { equities: syms, eqForTS, day, backfillMins, limit, moneyness, options_count: 0 }
      });
    }

    // ---- TRADIER (default) ----
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
      headers: H_JSON, params: { symbols: slice.join(",") }
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
    const { data } = await axios.get(`${TRADIER_BASE}/markets/options/chains`, { headers: H_JSON, params: { symbol: root, expiration: near } });
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

// GET /alpaca/scan
// Query: by=volume|trade_count  top=25  refresh=1|0 (default 1)
app.get("/alpaca/scan", async (req, res) => {
  try {
    const by = String(req.query.by || "volume");
    const top = Number(req.query.top || 30);
    const refresh = req.query.refresh === "0" ? 0 : 1;

    if (!APCA_API_KEY_ID || !APCA_API_SECRET_KEY) {
      return res.status(400).json({ ok: false, error: "Missing APCA_API_KEY_ID / APCA_API_SECRET_KEY" });
    }

    let payload;
    if (refresh) {
      // // Pull most-actives + movers
      // const [actives, gainers, losers] = await Promise.all([
      //   alpacaMostActives({ by, top }),
      //   alpacaMovers({ direction: "gainers", top }),
      //   alpacaMovers({ direction: "losers", top })
      // ]);

      // Pull most-actives + movers (both lists in one call)
      const [actives, mv] = await Promise.all([
        alpacaMostActives({ by, top }),
        alpacaMovers({ top })
      ]);
      const gainers = mv.gainers;
      const losers  = mv.losers;

      // Enrich with prev-day volume ratio (for a 'high vol' feel)
      const wanted = [
        ...new Set([
          ...actives.map(x => x.symbol),
          ...gainers.map(x => x.symbol),
          ...losers.map(x => x.symbol),
        ])
      ];
      const snaps = wanted.length ? await alpacaSnapshots(wanted) : {};
      const activesE = enrichWithPrevVol(actives, snaps);
      const gainersE = enrichWithPrevVol(gainers, snaps);
      const losersE  = enrichWithPrevVol(losers,  snaps);

      payload = {
        ok: true,
        ts: Date.now(),
        params: { by, top },
        groups: {
          most_actives: activesE,
          gainers: gainersE,
          losers: losersE
        }
      };
      //pushHistory({ most_actives_by: by, most_actives: activesE, gainers: gainersE, losers: losersE });
      pushHistory({ most_actives_by: by, most_actives: activesE, gainers: gainersE, losers: losersE });
    } else {
      // serve cached latest
      const latest = ALP_HISTORY[ALP_HISTORY.length - 1];
      payload = latest ? { ok: true, ts: latest.ts, params: { by: latest.most_actives_by, top }, groups: {
        most_actives: latest.most_actives,
        gainers: latest.gainers,
        losers: latest.losers
      }} : { ok:false, error:"no cached scan yet" };
    }

    // Also include a light history summary (symbol hit counts over last 7d)
    const hitCount = {};
    for (const snap of ALP_HISTORY) {
      for (const r of (snap.most_actives || [])) hitCount[r.symbol] = (hitCount[r.symbol] || 0) + 1;
      for (const r of (snap.gainers || []))      hitCount[r.symbol] = (hitCount[r.symbol] || 0) + 1;
      for (const r of (snap.losers || []))       hitCount[r.symbol] = (hitCount[r.symbol] || 0) + 1;
    }
    payload.history = {
      days: 7,
      samples: ALP_HISTORY.length,
      top_hits: Object.entries(hitCount)
        .sort((a,b)=>b[1]-a[1])
        .slice(0, 50)
        .map(([symbol, hits]) => ({ symbol, hits }))
    };

    res.json(payload);
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

/* ------------------------ boot ------------------------ */
app.listen(Number(PORT), () => {
  console.log(`HTTP on ${PORT} | WS on ${WS_PORT}`);
  console.log(`Try: curl "http://localhost:${PORT}/scan?limit=15"`);
  console.log(`Try: curl "http://localhost:${PORT}/watch?symbols=NVDA&eqForTS=NVDA&backfill=10"`);
});

