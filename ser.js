// server.js (ESM)
import "dotenv/config";
import express from "express";
import cors from "cors";
import { WebSocketServer } from "ws";
import * as ES from "eventsource";          // handle v1/v2
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

// Parse ?day=YYYY-MM-DD; default = today, but if weekend use last Friday
function parseDay(req) {
  const d = String(req.query.day || "").trim();
  if (d && /^\d{4}-\d{2}-\d{2}$/.test(d)) return d;
  const nowNY = DateTime.now().setZone("America/New_York");
  if (nowNY.weekday === 6) return nowNY.minus({ days: 1 }).toFormat("yyyy-LL-dd"); // Sat -> Fri
  if (nowNY.weekday === 7) return nowNY.minus({ days: 2 }).toFormat("yyyy-LL-dd"); // Sun -> Fri
  return nowNY.toFormat("yyyy-LL-dd");
}

// ---------- Time & Sales (backfill small window) ----------
async function backfillEquityTS(symbol, day, minutes = 5) {
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
      // tick = { time: "2025-10-24 09:31:00", price, size }
      broadcast({ type: "equity_ts", symbol, data: tick });
    });
  } catch (e) {
    console.error("backfillEquityTS error:", errToJson(e));
  }
}

// ---------- Quotes SSE (equities only for simplicity) ----------
let esQuotes = null;

function startQuoteStream(symbols = []) {
  try {
    if (esQuotes) esQuotes.close();
    const uniq = [...new Set(symbols)].filter(Boolean);
    if (uniq.length === 0) return;

    const url = `${TRADIER_BASE}/markets/events?symbols=${encodeURIComponent(uniq.join(","))}&sessionid=${Date.now()}`;
    const es  = new EventSource(url, { headers: H_SSE });
    esQuotes  = es;

    es.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg?.type === "quote" && msg?.data) {
          const q = msg.data; // {symbol, last, bid, ask, size, ...}
          broadcast({ type: "quotes", data: q });

          // If the symbol happens to be an OCC option, you could emit synthetic option_ts here
          // (Keeping it equities-only for now)
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
// Example:
//   http://localhost:8080/watch?symbols=SPY&eqForTS=SPY&backfill=15&day=2025-10-24
app.get("/watch", async (req, res) => {
  try {
    const symbols = String(req.query.symbols || "SPY")
      .split(",").map(s => s.trim().toUpperCase()).filter(Boolean);

    const eqForTS = String(req.query.eqForTS || symbols.join(","))
      .split(",").map(s => s.trim().toUpperCase()).filter(Boolean);

    const backfillMins = req.query.backfill ? Number(req.query.backfill) : 5;
    const day = parseDay(req);

    // Start streaming quotes for equities
    startQuoteStream(symbols);

    // Backfill a small T&S window for each requested equity (e.g., SPY)
    if (backfillMins > 0) {
      await Promise.all(eqForTS.map(sym => backfillEquityTS(sym, day, backfillMins)));
    }

    res.json({
      ok: true,
      env: { base: TRADIER_BASE, sandbox: isSandbox, ts_interval: TS_INTERVAL },
      watching: { equities: symbols, eqForTS, day, backfillMins }
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
  console.log(`Try: curl "http://localhost:${PORT}/watch?symbols=SPY&eqForTS=SPY&backfill=15&day=2025-10-24"`);
});

