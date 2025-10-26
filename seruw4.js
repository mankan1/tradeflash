// server/ser.js
import 'dotenv/config';
import express from 'express';
import axios from 'axios';
// ESM-safe EventSource shim (handles CJS and ESM builds)
import * as _ES from 'eventsource';
const EventSource = (_ES.default ?? _ES.EventSource ?? _ES);

import { WebSocketServer } from 'ws';
import { DateTime } from 'luxon';

/* ========= ENV ========= */
const PORT = process.env.HTTP_PORT || 8080;
const WS_PORT = process.env.WS_PORT || 8081;

const TRADIER_BASE = (process.env.TRADIER_BASE || 'https://api.tradier.com/v1').replace(/\/+$/,'');
const TRADIER_TOKEN = process.env.TRADIER_TOKEN;
if (!TRADIER_TOKEN) {
  console.error('Missing TRADIER_TOKEN in .env');
  process.exit(1);
}
const H_JSON = { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: 'application/json' };
const H_SSE  = { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: 'text/event-stream' };

/* ========= STATE ========= */
const app = express();
app.use((_req, res, next) => { res.setHeader('Access-Control-Allow-Origin', '*'); next(); });

const wss = new WebSocketServer({ port: Number(WS_PORT) });
function broadcast(obj) {
  const s = JSON.stringify(obj);
  for (const c of wss.clients) { if (c.readyState === 1) c.send(s); }
}

/** live book cache for equities & options */
const midBySym = new Map(); // sym -> {bid, ask, mid, ts}
const lastTradeBySym = new Map(); // sym -> last trade price

/** option-specific caches */
const volByOcc = new Map(); // OCC -> cumulative day volume we’ve seen
const oiByOcc  = new Map(); // OCC -> open interest (from chains)

/* ========= UTIL ========= */
function epsFor(book) {
  const b = Number(book?.bid || 0);
  const a = Number(book?.ask || 0);
  if (a && b && a > b) return Math.max(0.005, (a - b) / 4); // quarter-spread, min 0.5c
  return 0.01; // 1c catch-all
}

function updateMidFromQuote(q) {
  // q has { symbol, bid, ask, last, ... } for both equities and options
  const bid = Number(q.bid ?? 0);
  const ask = Number(q.ask ?? 0);
  const mid = (bid && ask) ? (bid + ask) / 2 : (Number(q.last ?? 0) || undefined);
  midBySym.set(q.symbol, { bid: bid || undefined, ask: ask || undefined, mid, ts: Date.now() });
  // also keep for root if equity
  if (!/^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(q.symbol)) {
    midBySym.set(q.symbol.replace(/[^\w]+/g,''), { bid: bid || undefined, ask: ask || undefined, mid, ts: Date.now() });
  }
}

function inferSideServer(sym, price) {
  const book = midBySym.get(sym) || {};
  const eps = epsFor(book);
  if (book.ask && price >= book.ask - eps) return { side: 'BOT', side_src: 'mid' };
  if (book.bid && price <= book.bid + eps) return { side: 'SLD', side_src: 'mid' };

  // mid band
  if (book.mid && Math.abs(price - book.mid) <= eps) {
    // no direct side from mid; fallback to tick
  }

  // tick fallback
  const last = lastTradeBySym.get(sym);
  if (typeof last === 'number') {
    if (price > last) return { side: 'BOT', side_src: 'tick' };
    if (price < last) return { side: 'SLD', side_src: 'tick' };
  }
  return { side: '—', side_src: 'none' };
}

/** Classify Open/Close intent */
function classifyOpenClose({ qty, oi, priorVol, side, at }) {
  // If we can’t decide side, we can’t map to BTO/STO/BTC/STC
  if (side !== 'BOT' && side !== 'SLD') return { action: '—', action_conf: 'low' };

  let action = '—';
  let action_conf = 'low';

  const haveOI = Number.isFinite(oi);
  const haveVol = Number.isFinite(priorVol);

  if (haveOI && haveVol) {
    // Open (high): qty > (OI + dayVol)
    if (qty > (oi + priorVol)) {
      action = side === 'BOT' ? 'BTO' : 'STO';
      action_conf = 'high';
      return { action, action_conf };
    }
    // Close lean (medium): dayVol ≳ 80% of OI
    if (priorVol >= 0.8 * oi) {
      action = side === 'BOT' ? 'BTC' : 'STC';
      action_conf = 'medium';
      return { action, action_conf };
    }
  }

  // When we can’t achieve the above thresholds:
  // Provide a low-confidence hint based on side + edge
  if (side === 'BOT') {
    // buyer-aggress more likely opening longs unless volume already heavy vs OI
    action = 'OPEN?';
  } else if (side === 'SLD') {
    action = 'OPEN?';
  }
  // if it was at bid, seller-aggress at bid often STC, buyer-aggress at ask often BTO — but we keep these as hints
  if (at === 'bid' && side === 'SLD') action = 'CLOSE?';
  if (at === 'ask' && side === 'BOT') action = 'OPEN?';

  return { action, action_conf };
}

/* ========= OPTION OI REFRESH ========= */
async function refreshOIForRoot(root, { moneyness = 0.25, expiries = [] } = {}) {
  // Pull chains for a couple of expiries; cache OI per OCC
  try {
    // If expiries were not given, ask Tradier for nearest expiry list first
    let exps = expiries;
    if (!exps || !exps.length) {
      const { data: e } = await axios.get(`${TRADIER_BASE}/markets/options/expirations`, {
        headers: H_JSON, params: { symbol: root, includeAllRoots: 'true', strikes: 'false' }
      });
      exps = (e?.expirations?.expiration ?? []).slice(0, 2); // nearest couple
    }

    for (const exp of exps) {
      const { data } = await axios.get(`${TRADIER_BASE}/markets/options/chains`, {
        headers: H_JSON,
        params: { symbol: root, expiration: exp, greeks: 'false' }
      });
      const list = data?.options?.option ?? [];
      if (!list.length) continue;

      // Filter by moneyness if we have underlying mid
      const book = midBySym.get(root) || {};
      const spot = Number(book?.mid || book?.bid || book?.ask || NaN);
      const out = list.filter(o => {
        if (!Number.isFinite(spot)) return true;
        const strike = Number(o.strike);
        if (!Number.isFinite(strike)) return true;
        const ratio = Math.abs(strike - spot) / spot;
        return ratio <= moneyness;
      });

      for (const o of out) {
        const occ = o.symbol;
        const oi = Number(o.open_interest ?? o.openInterest ?? NaN);
        if (Number.isFinite(oi)) oiByOcc.set(occ, oi);
      }
    }
  } catch (e) {
    console.error('refreshOIForRoot error:', e?.response?.data || e.message);
  }
}

/* ========= STREAMS ========= */
let esQuotes = null;

function startQuoteStream(symbolsOrOCC = []) {
  try {
    if (esQuotes) esQuotes.close();
    const uniq = [...new Set(symbolsOrOCC)].filter(Boolean);
    if (!uniq.length) return;

    const url = `${TRADIER_BASE}/markets/events?symbols=${encodeURIComponent(uniq.join(','))}&sessionid=${Date.now()}`;
    const es = new EventSource(url, { headers: H_SSE });
    esQuotes = es;

    es.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        if (msg?.type !== 'quote' || !msg?.data) return;

        const q = msg.data;
        updateMidFromQuote(q);

        // Only process trades
        const size = Number(q.size ?? q.volume ?? 0);
        const price = Number(q.last ?? 0);
        if (!(price > 0 && size > 0)) return;

        const sym = q.symbol;
        const isOCC = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(sym);
        const root = isOCC ? sym.replace(/\d{6}[CP]\d{8}$/i, '') : sym;

        // server-side side inference
        const { side, side_src } = inferSideServer(isOCC ? sym : root, price);
        lastTradeBySym.set(isOCC ? sym : root, price);

        // Always broadcast raw quote as telemetry
        broadcast({ type: 'quotes', data: q, side, side_src });

        // Compute 'at' from live book
        const book = midBySym.get(isOCC ? sym : root) || {};
        const eps = epsFor(book);
        let at = 'between';
        if (book.ask && price >= book.ask - eps) at = 'ask';
        else if (book.bid && price <= book.bid + eps) at = 'bid';
        else if (book.mid && Math.abs(price - book.mid) <= eps) at = 'mid';

        if (isOCC) {
          // keep day volume per OCC and compute priorVol
          const seenVol = Number(q.volume ?? NaN);
          let priorVol = volByOcc.has(sym) ? volByOcc.get(sym) : 0;
          if (Number.isFinite(seenVol)) priorVol = Math.max(0, seenVol - size);
          if (Number.isFinite(seenVol)) volByOcc.set(sym, seenVol);
          else volByOcc.set(sym, (volByOcc.get(sym) || 0) + size);

          const oi = oiByOcc.get(sym) ?? null;
          const { action, action_conf } = classifyOpenClose({ qty: size, oi, priorVol, side, at });

          broadcast({
            type: 'option_ts',
            symbol: sym,
            data: {
              id: `ots_${sym}_${q.trade_time || Date.now()}`,
              ts: Date.now(),
              option: { expiry: '', strike: 0, right: /C\d{8}$/i.test(sym) ? 'C' : 'P' },
              qty: size,
              price,
              side, side_src,
              oi, priorVol, at, book,
              action, action_conf
            }
          });
        } else {
          broadcast({
            type: 'equity_ts',
            symbol: root,
            data: {
              time: q.trade_time,
              price,
              size,
              side, side_src,
              at, book
            }
          });
        }
      } catch { /* ignore single bad frame */ }
    };

    es.onerror = () => {
      try { es.close(); } catch {}
      setTimeout(() => startQuoteStream(uniq), 1500);
    };
  } catch (e) {
    console.error('startQuoteStream error:', e?.message || e);
  }
}

/* ========= BACKFILL ========= */
const TS_INTERVAL = process.env.TS_INTERVAL || 'tick';

async function backfillEquityTS(symbol, day, minutes = 5) {
  try {
    const start = DateTime.fromISO(`${day}T09:30:00`, { zone: 'America/New_York' });
    const end = start.plus({ minutes });
    const { data } = await axios.get(`${TRADIER_BASE}/markets/timesales`, {
      headers: H_JSON,
      params: {
        symbol, interval: TS_INTERVAL,
        start: start.toFormat('yyyy-LL-dd HH:mm:ss'),
        end: end.toFormat('yyyy-LL-dd HH:mm:ss')
      }
    });
    const series = data?.series?.data ?? [];
    for (const t of series) {
      const price = Number(t.price ?? t.last ?? t.close ?? 0);
      const size = Number(t.size ?? t.volume ?? t.qty ?? t.quantity ?? 0);
      if (!(price > 0 && size > 0)) continue;

      const { side, side_src } = inferSideServer(symbol, price);
      lastTradeBySym.set(symbol, price);

      const book = midBySym.get(symbol) || {};
      const eps = epsFor(book);
      let at = 'between';
      if (book.ask && price >= book.ask - eps) at = 'ask';
      else if (book.bid && price <= book.bid + eps) at = 'bid';
      else if (book.mid && Math.abs(price - book.mid) <= eps) at = 'mid';

      broadcast({ type: 'equity_ts', symbol, data: { ...t, price, size, side, side_src, at } });
    }
  } catch (e) {
    console.error('backfillEquityTS error:', e?.response?.data || e.message);
  }
}

/* ========= ROUTES ========= */
app.get('/watch', async (req, res) => {
  try {
    const symbols = String(req.query.symbols || '').split(',').map(s => s.trim().toUpperCase()).filter(Boolean);
    const eqForTS = String(req.query.eqForTS || '').split(',').map(s => s.trim().toUpperCase()).filter(Boolean);
    const limit = Number(req.query.limit || 200);
    const moneyness = Number(req.query.moneyness || 0.25);
    const backfill = Number(req.query.backfill || 5);
    const day = req.query.day ? String(req.query.day) : null;

    // kick option OI refresh for each root (best-effort)
    for (const root of symbols) {
      refreshOIForRoot(root, { moneyness }).catch(()=>{});
    }

    // Start streaming quotes/trades for all requested roots
    startQuoteStream(symbols);

    if (day && backfill > 0 && eqForTS.length) {
      for (const s of eqForTS) await backfillEquityTS(s, day, backfill);
    }

    res.json({
      ok: true,
      env: { base: TRADIER_BASE, sandbox: /sandbox/i.test(TRADIER_BASE), ts_interval: TS_INTERVAL },
      watching: { equities: symbols, eqForTS, day: day || undefined, backfillMins: backfill }
    });
  } catch (e) {
    console.error('/watch error:', e?.message || e);
    res.status(500).json({ ok: false, error: e?.message || 'watch failed' });
  }
});

/* ========= START ========= */
app.listen(Number(PORT), () => {
  console.log(`HTTP on ${PORT} | WS on ${WS_PORT}`);
  console.log('Hit /watch to start streams, e.g.:');
  console.log(`  curl "http://localhost:${PORT}/watch?symbols=SPY,QQQ&eqForTS=SPY&backfill=5"`);
});

