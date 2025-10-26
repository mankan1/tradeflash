import "dotenv/config";
import express from "express";
import cors from "cors";
import { WebSocketServer } from "ws";
import * as ES from "eventsource"; const EventSource = ES.default ?? ES;
import axios from "axios";
import { DateTime } from "luxon";

const { TRADIER_BASE="", TRADIER_TOKEN="", PORT=8080, WS_PORT=8081 } = process.env;
if (!TRADIER_BASE || !TRADIER_TOKEN) throw new Error("Missing TRADIER_* envs");

const app = express();
app.use(cors());

const wss = new WebSocketServer({ port: Number(WS_PORT) });
const broadcast = (msg) => {
  const s = JSON.stringify(msg);
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
};

const H_JSON = { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: "application/json" };
const H_SSE  = { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: "text/event-stream" };
const errToJson = (e) => (e?.response ? { status: e.response.status, data: e.response.data } : { message: String(e) });
const isSandbox = TRADIER_BASE.includes("sandbox");
const TS_INTERVAL = isSandbox ? "1min" : "tick";

function parseDay(req) {
  const d = String(req.query.day || "").trim();
  if (d && /^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
  const nowNY = DateTime.now().setZone("America/New_York");
  if (nowNY.weekday === 6) return nowNY.minus({ days: 1 }).toFormat("yyyy-LL-dd"); // Sat->Fri
  if (nowNY.weekday === 7) return nowNY.minus({ days: 2 }).toFormat("yyyy-LL-dd"); // Sun->Fri
  return nowNY.toFormat("yyyy-LL-dd");
}

// ---- side inference + books ----
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
  if (Number.isFinite(bid) && Number.isFinite(ask)) return Math.max((ask - bid) * 0.15, 0.01); // 15% of spread
  if (Number.isFinite(mid)) return Math.max(0.001 * mid, 0.01); // 0.1% or a cent
  return 0.01;
};
function inferSideServer(sym, price) {
  const book = midBySym.get(sym);
  if (book) {
    const { bid, ask, mid } = book;
    const eps = epsFor(book);
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

// ---- NEW: per-OCC caches for OI/volume ----
const oiByOcc = new Map();   // OCC -> open_interest
const volByOcc = new Map();  // OCC -> last seen cumulative volume

// ---- equity T&S backfill (short window) ----
async function backfillEquityTS(symbol, day, minutes=5) {
  try {
    const start = DateTime.fromISO(`${day}T09:30:00`, { zone: "America/New_York" });
    const end   = start.plus({ minutes });
    const params = {
      symbol, interval: TS_INTERVAL,
      start: start.toFormat("yyyy-LL-dd HH:mm:ss"),
      end:   end.toFormat("yyyy-LL-dd HH:mm:ss"),
    };
    const { data } = await axios.get(`${TRADIER_BASE}/markets/timesales`, { headers: H_JSON, params });
    (data?.series?.data ?? []).forEach(tick => {
      const price = Number(tick.price ?? tick.last ?? tick.close ?? 0);
      const { side, side_src } = inferSideServer(symbol, price);
      lastTradeBySym.set(symbol, price);

      // try classify where it hit relative to current book
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

// ---- options helpers (exp -> chains -> OCC) ----
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
      // NEW: cache OI and starting day volume if provided
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

// ---- quotes SSE (equities + OCC) ----
let esQuotes = null;
function startQuoteStream(symbolsOrOCC=[]) {
  try {
    if (esQuotes) esQuotes.close();
    const uniq = [...new Set(symbolsOrOCC)].filter(Boolean);
    if (uniq.length === 0) return;
    const url = `${TRADIER_BASE}/markets/events?symbols=${encodeURIComponent(uniq.join(","))}&sessionid=${Date.now()}`;
    const es  = new EventSource(url, { headers: H_SSE });
    esQuotes  = es;

    es.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg?.type === "quote" && msg?.data) {
          const q = msg.data; // {symbol, last, bid, ask, size, volume, ...}

          // keep books (for equity AND option symbols)
          updateMidFromQuote(q);

          if (q.last && q.size && q.size > 0) {
            // ---- broadcast trade-like quote w/ side for root symbol (used by Trade Flash) ----
            const rootSym = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(q.symbol)
              ? q.symbol.replace(/\d{6}[CP]\d{8}$/, "")
              : q.symbol;

            const price = Number(q.last);
            const { side, side_src } = inferSideServer(rootSym, price);
            lastTradeBySym.set(rootSym, price);
            broadcast({ type: "quotes", data: q, side, side_src });

            // ---- if OCC option, also emit enriched option T&S ----
            const isOCC = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(q.symbol);
            if (isOCC) {
              // side using the OPTION's own book:
              const optSide = inferSideServer(q.symbol, price);
              const book = midBySym.get(q.symbol) || {};
              const eps = epsFor(book);
              let at = "between";
              if (book.ask && price >= book.ask - eps) at = "ask";
              else if (book.bid && price <= book.bid + eps) at = "bid";
              else if (book.mid && Math.abs(price - book.mid) <= eps) at = "mid";

              // priorVol from cumulative 'volume' if present, else our cache
              const seenVol = Number(q.volume ?? NaN);
              let priorVol = volByOcc.has(q.symbol) ? volByOcc.get(q.symbol) : 0;
              if (Number.isFinite(seenVol)) priorVol = Math.max(0, seenVol - Number(q.size));
              // update last seen vol
              if (Number.isFinite(seenVol)) volByOcc.set(q.symbol, seenVol);
              else volByOcc.set(q.symbol, (volByOcc.get(q.symbol) || 0) + Number(q.size));

              const oi = oiByOcc.get(q.symbol) ?? null;

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
                  oi,                 // NEW
                  priorVol,           // NEW
                  book,               // NEW {bid, ask, mid}
                  at,                 // NEW "bid"|"ask"|"mid"|"between"
                }
              });
            }
          } else {
            broadcast({ type: "quotes", data: q }); // no trade info, still useful for book
          }
        }
      } catch { /* ignore */ }
    };
    es.onerror = () => { es.close(); setTimeout(() => startQuoteStream(uniq), 1500); };
  } catch (e) {
    console.error("startQuoteStream error:", errToJson(e));
  }
}

// ---- /watch ----
app.get("/watch", async (req, res) => {
  try {
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
      watching:{ equities:symbols, options_count:occSet.size, eqForTS, day, backfillMins, moneyness, limit, expiries } });
  } catch (e) {
    const detail = errToJson(e); console.error("/watch error:", detail);
    res.status(detail.status || 500).json({ ok:false, error: detail });
  }
});

app.listen(Number(PORT), () => {
  console.log(`HTTP on ${PORT} | WS on ${WS_PORT}`);
  console.log(`Try: curl "http://localhost:${PORT}/watch?symbols=NVDA&eqForTS=NVDA&backfill=10"`);
});

