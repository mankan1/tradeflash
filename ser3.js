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
    (data?.series?.data ?? []).forEach(tick => broadcast({ type: "equity_ts", symbol, data: tick }));
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
          const q = msg.data;
          broadcast({ type: "quotes", data: q });
          const isOCC = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(q.symbol);
          if (isOCC && q.last && q.size) {
            broadcast({ type: "option_ts", symbol: q.symbol, data: {
              id: `ots_${q.symbol}_${q.trade_time || Date.now()}`,
              ts: Date.now(),
              option: { expiry: "", strike: 0, right: /C\d{8}$/i.test(q.symbol) ? "C" : "P" },
              qty: q.size, price: +q.last } });
          }
        }
      } catch {}
    };
    es.onerror = () => { es.close(); setTimeout(() => startQuoteStream(uniq), 1500); };
  } catch (e) {
    console.error("startQuoteStream error:", errToJson(e));
  }
}

// ---- /watch ----
// Query: symbols=SPY,NVDA  eqForTS=NVDA  backfill=10  moneyness=0.25  limit=200  day=YYYY-MM-DD
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

