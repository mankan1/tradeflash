// server.js (ESM)
import "dotenv/config";
import express from "express";
import cors from "cors";
import { WebSocketServer } from "ws";
import * as ES from "eventsource";           // handle both default/named
const EventSource = ES.default ?? ES;
import axios from "axios";
import { DateTime } from "luxon";

// ---------- ENV ----------
const {
  TRADIER_BASE = "",
  TRADIER_TOKEN = "",
  PORT = 8080,
  WS_PORT = 8081,
} = process.env;

if (!TRADIER_BASE || !TRADIER_TOKEN) {
  throw new Error("Missing TRADIER_BASE or TRADIER_TOKEN in .env");
}

// ---------- APP / WS ----------
const app = express();
app.use(cors());

const wss = new WebSocketServer({ port: Number(WS_PORT) });
const broadcast = (msg) => {
  const s = JSON.stringify(msg);
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
};

// ---------- HEADERS ----------
const H_JSON = { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: "application/json" };
const H_SSE  = { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: "text/event-stream" };

// ---------- UTIL ----------
const errToJson = (e) => (e?.response ? { status: e.response.status, data: e.response.data } : { message: String(e) });
const isSandbox = TRADIER_BASE.includes("sandbox");
const TS_INTERVAL = isSandbox ? "1min" : "tick";
const nyDate = () => DateTime.now().setZone("America/New_York").toFormat("yyyy-LL-dd");

// ---------- Time & Sales (small backfill) ----------
async function backfillEquityTS(symbol, day = nyDate(), minutes = 5) {
  try {
    const start = DateTime.fromISO(`${day}T09:30:00`, { zone: "America/New_York" });
    const end   = start.plus({ minutes });
    const params = {
      symbol,
      interval: TS_INTERVAL,
      start: start.toFormat("yyyy-LL-dd HH:mm:ss"),
      end:   end.toFormat("yyyy-LL-dd HH:mm:ss"),
    };
    const { data } = await axios.get(`${TRADIER_BASE}/markets/timesales`, { headers: H_JSON, params });
    (data?.series?.data ?? []).forEach(tick => {
      broadcast({ type: "equity_ts", symbol, data: tick });
    });
  } catch (e) {
    console.error("backfillEquityTS error:", errToJson(e));
  }
}

// ---------- Quotes SSE (equities + OCC if you include them) ----------
let esQuotes = null;

function startQuoteStream(symbolsOrOCC = []) {
  try {
    if (esQuotes) esQuotes.close();

    const uniq = [...new Set(symbolsOrOCC)].filter(Boolean);
    const url  = `${TRADIER_BASE}/markets/events` +
                 `?symbols=${encodeURIComponent(uniq.join(","))}` +
                 `&sessionid=${Date.now()}`;

    const es = new EventSource(url, { headers: H_SSE });
    esQuotes = es;

    es.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg?.type === "quote" && msg?.data) {
          const q = msg.data; // {symbol, last, size, ...}
          broadcast({ type: "quotes", data: q });

          const isOCC = /^[A-Z]{1,6}\d{6}[CP]\d{8}$/i.test(q.symbol);
          if (isOCC && q.last && q.size) {
            broadcast({
              type: "option_ts",
              symbol: q.symbol,
              data: {
                id: `ots_${q.symbol}_${q.trade_time || Date.now()}`,
                ts: Date.now(),
                option: { expiry: "", strike: 0, right: /C\d{8}$/i.test(q.symbol) ? "C" : "P" },
                qty: q.size,
                price: +q.last
              }
            });
          }
        }
      } catch { /* ignore */ }
    };

    es.onerror = () => {
      es.close();
      setTimeout(() => startQuoteStream(uniq), 1500);
    };
  } catch (e) {
    console.error("startQuoteStream error:", errToJson(e));
  }
}

// ---------- /watch ----------
app.get("/watch", async (req, res) => {
  try {
    const symbols = String(req.query.symbols || "SPY,QQQ")
      .split(",").map(s => s.trim().toUpperCase()).filter(Boolean);

    // Start SSE quotes for just the equities for now (keep it simple)
    startQuoteStream(symbols);

    // Seed a short backfill so the UI isn't empty
    const day = nyDate();
    const backfillMins = req.query.backfill ? Number(req.query.backfill) : 5;
    if (backfillMins > 0) {
      await Promise.all(symbols.map(sym => backfillEquityTS(sym, day, backfillMins)));
    }

    res.json({
      ok: true,
      env: { base: TRADIER_BASE, sandbox: isSandbox, ts_interval: TS_INTERVAL },
      watching: { equities: symbols, backfillMins }
    });
  } catch (e) {
    const detail = errToJson(e);
    console.error("/watch error:", detail);
    res.status(detail.status || 500).json({ ok: false, error: detail });
  }
});

// ---------- START ----------
app.listen(Number(PORT), () => {
  console.log(`HTTP on ${PORT} | WS on ${WS_PORT}`);
  console.log(`curl "http://localhost:${PORT}/watch?symbols=SPY,QQQ&backfill=5"`);
});

