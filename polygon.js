// server/providers/polygon.js
import axios from "axios";
import { AsyncLocalStorage } from "node:async_hooks";


export const reqStore = new AsyncLocalStorage();


// --- ENV & axios ---
const { POLYGON_KEY = "" } = process.env;
if (!POLYGON_KEY) {
  console.warn("[polygon] POLYGON_KEY is not set; polygon provider will error on use.");
}

export const POLY = axios.create({
  baseURL: "https://api.polygon.io",
  headers: { Authorization: `Bearer ${POLYGON_KEY}` },
  timeout: 15000,
});

attachAxiosLogging(POLY, "polygon");       // your Polygon axios instance

// Helpers
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const num = (x) => (Number.isFinite(+x) ? +x : 0);

// Maintain book + last timestamps for polling
const midBySym = new Map();           // e.g. "SPY" or "O:SPY..." -> { bid, ask, mid }
const lastTradeTsNs = new Map();      // ticker -> last nanosecond ts we emitted (for /v3/trades polling)
const lastUnderTradePrice = new Map();// root equity -> last price (for client uptick fallback if needed)

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

// ---- Update NBBO -> mid cache from a /v3/quotes row ----
function updateMidFromQuoteRow(ticker, qrow) {
  const bid = num(qrow?.bid_price ?? qrow?.bp);
  const ask = num(qrow?.ask_price ?? qrow?.ap);
  if (!Number.isFinite(bid) && !Number.isFinite(ask)) return;
  const prev = midBySym.get(ticker) || {};
  const newBid = Number.isFinite(bid) ? bid : prev.bid;
  const newAsk = Number.isFinite(ask) ? ask : prev.ask;
  const mid = (Number.isFinite(newBid) && Number.isFinite(newAsk)) ? (newBid + newAsk) / 2 : prev.mid;
  if (Number.isFinite(newBid) || Number.isFinite(newAsk) || Number.isFinite(mid)) {
    midBySym.set(ticker, { bid: newBid, ask: newAsk, mid });
  }
}
const epsFor = ({ mid, bid, ask }) => {
  if (Number.isFinite(bid) && Number.isFinite(ask)) return Math.max((ask - bid) * 0.15, 0.01);
  if (Number.isFinite(mid)) return Math.max(0.001 * mid, 0.01);
  return 0.01;
};
function inferSideFromBook(ticker, price) {
  const book = midBySym.get(ticker) || {};
  const eps = epsFor(book);
  if (Number.isFinite(book.ask) && price >= (book.ask - eps)) return { side:"BOT", side_src:"mid", at:"ask", book };
  if (Number.isFinite(book.bid) && price <= (book.bid + eps)) return { side:"SLD", side_src:"mid", at:"bid", book };
  if (Number.isFinite(book.mid)) {
    if (price > book.mid + eps) return { side:"BOT", side_src:"mid", at:"between", book };
    if (price < book.mid - eps) return { side:"SLD", side_src:"mid", at:"between", book };
    return { side:"—", side_src:"mid", at:"mid", book };
  }
  return { side:"—", side_src:"none", at:"between", book };
}

// ---- Polygon endpoints we’ll use ----
// Stocks trades:   GET /v3/trades/{stockTicker}            (tick-level)        :contentReference[oaicite:0]{index=0}
// Stocks quotes:   GET /v3/quotes/{stockTicker}            (NBBO snapshots)    :contentReference[oaicite:1]{index=1}
// Stock snapshot:  GET /v2/snapshot/locale/us/markets/stocks/tickers/{tkr}     :contentReference[oaicite:2]{index=2}
// Options trades:  GET /v3/trades/{optionsTicker}          (tick-level)        :contentReference[oaicite:3]{index=3}
// Options quotes:  GET /v3/quotes/{optionsTicker}                             :contentReference[oaicite:4]{index=4}
// Options chain:   GET /v3/snapshot/options/{underlying}   (includes OI, vols) :contentReference[oaicite:5]{index=5}

// Build watch list of option contracts near the money (moneyness ±) & limited count
async function buildOptionListPolygon(under, { moneyness = 0.25, limit = 150 } = {}) {
  // Get underlying reference (last price) via single-ticker snapshot
  const snap = await POLY.get(`/v2/snapshot/locale/us/markets/stocks/tickers/${encodeURIComponent(under)}`);
  const undLast = num(snap?.data?.ticker?.lastTrade?.p) || num(snap?.data?.ticker?.min?.o) || 0;

  // Pull chain snapshot (delayed on Starter/Developer plans).
  // We’ll page up to 250 per page and filter to strikes within ±moneyness
  const out = [];
  let pageURL = `/v3/snapshot/options/${encodeURIComponent(under)}?limit=250`;
  while (pageURL && out.length < limit) {
    const { data } = await POLY.get(pageURL);
    const rows = data?.results || [];
    for (const r of rows) {
      const tk = r?.details?.ticker;          // e.g. "O:SPY20251115C00670000"
      const strike = num(r?.details?.strike_price);
      if (!tk || !strike) continue;
      const within = undLast ? (Math.abs(strike - undLast) / undLast) <= moneyness : true;
      if (within) out.push(tk);
      if (out.length >= limit) break;
    }
    pageURL = data?.next_url ? data.next_url.replace("https://api.polygon.io","") : null;
  }
  return out.slice(0, limit);
}

// Latest NBBO quote for a ticker (stock or option), newest first
async function fetchLatestQuote(ticker) {
  const { data } = await POLY.get(`/v3/quotes/${encodeURIComponent(ticker)}`, {
    params: { limit: 1, order: "desc", sort: "timestamp" },
  });
  const q = (data?.results && data.results[0]) || null;
  if (q) updateMidFromQuoteRow(ticker, q);
  return q;
}

// Poll trades incrementally (stocks or options)
async function fetchNewTrades(ticker) {
  const since = lastTradeTsNs.get(ticker); // nanoseconds
  const params = {
    limit: 1000,
    order: "asc",
    sort: "timestamp",
  };
  if (since) params.timestamp_gt = String(since);
  const { data } = await POLY.get(`/v3/trades/${encodeURIComponent(ticker)}`, { params });
  const rows = data?.results || [];
  if (rows.length) {
    const last = rows[rows.length - 1];
    const ts = Number(last?.sip_timestamp ?? last?.t ?? last?.timestamp ?? 0);
    if (ts) lastTradeTsNs.set(ticker, ts);
  }
  return rows;
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

async function polyUOAForRoot(root, { moneyness = 0.2, minVol = 500 } = {}) {
  // /v3/snapshot/options/{underlying}
  // (This returns many contracts; filter by moneyness around underlying)
  const snap = await POLY.get(`/v3/snapshot/options/${encodeURIComponent(root)}`);
  const und = Number(snap.data?.underlying_asset?.price ?? 0);
  const arr = Array.isArray(snap.data?.results) ? snap.data.results : [];

  const within = (o) => {
    if (!und) return true;
    const strike = Number(o.details?.strike_price ?? o.details?.strike ?? 0);
    return und > 0 ? Math.abs(strike - und) / und <= moneyness : true;
  };

  const flagged = arr
    .map(o => ({
      occ: o.ticker, // Polygon options ticker e.g. O:NVDA241101C00450000
      vol: Number(o.day?.volume ?? 0),
      oi:  Number(o.open_interest ?? 0),
      last: Number(o.last_trade?.price ?? 0),
      strike: Number(o.details?.strike_price ?? 0),
      right: String(o.details?.contract_type ?? "call").toUpperCase().startsWith("C") ? "C" : "P",
    }))
    .filter(x => x.vol >= minVol && x.vol > x.oi && within({ details: { strike_price: x.strike } }))
    .sort((a,b)=> (b.vol||0)-(a.vol||0))
    .slice(0, 3);

  return { count: flagged.length, top: flagged };
}

// Public API: start polygon “watch” (polling loops that broadcast WS frames)
export async function startPolygonWatch({
  equities = [],
  moneyness = 0.25,
  limit = 150,
  broadcast,           // (msg) => void
  classifyOptionAction, // ({ qty, oi, priorVol, side, at }) => { action, action_conf }
}) {
  // 1) Build option universe from chain snapshot
  const optionTickers = new Set();
  for (const root of equities) {
    try {
      const occ = await buildOptionListPolygon(root, { moneyness, limit: Math.ceil(limit / Math.max(1, equities.length)) });
      occ.forEach(t => optionTickers.add(t));
    } catch (e) {
      console.warn("[polygon] chain build failed", root, e?.response?.status || e?.message);
    }
  }

  // 2) seed books with one NBBO fetch per ticker (equity + options)
  const allTks = [...new Set([...equities, ...optionTickers])];
  await Promise.all(allTks.map(tk => fetchLatestQuote(tk).catch(()=>{})));

  // 3) Create polling loops
  let alive = true;

  // Poll quotes (books) periodically (lighter weight)
  (async function loopQuotes() {
    while (alive) {
      try {
        await Promise.all(allTks.map(tk => fetchLatestQuote(tk)));
      } catch {}
      await sleep(1500);
    }
  })();

  // Poll trades incrementally; translate to your frames
  (async function loopTrades() {
    while (alive) {
      try {
        // STOCKS
        for (const sym of equities) {
          const qrows = await fetchNewTrades(sym);
          for (const t of qrows) {
            const price = num(t.price ?? t.p);
            const size  = num(t.size  ?? t.s);
            if (!(price > 0 && size > 0)) continue;
            // Remember last equity price for client uptick fallback
            lastUnderTradePrice.set(sym, price);

            const { side, side_src, at, book } = inferSideFromBook(sym, price);
            broadcast({
              type: "equity_ts",
              symbol: sym,
              data: {
                time: new Date(Number(t.sip_timestamp ?? t.timestamp ?? Date.now()) / 1e6).toISOString(),
                price,
                size,
                side,
                side_src,
                at,
                book
              }
            });
          }
          // also broadcast a lightweight 'quotes' like you do for Tradier (if you want parity):
          // not necessary for equities, but keeps client logic uniform
        }

        // OPTIONS
        for (const otk of optionTickers) {
          const trows = await fetchNewTrades(otk);
          if (!trows.length) continue;

          // keep an NBBO for option (used for 'at' and side)
          const book = midBySym.get(otk) || {};
          const eps = epsFor(book);

          // We also want OI + dayVol for action tags -> pull from option contract snapshot once a loop
          let oi = null, dayVol = null;
          try {
            const { data: snap } = await POLY.get(`/v3/snapshot/options/${encodeURIComponent(otk)}`);
            oi = num(snap?.results?.open_interest);
            // “day” can be under results.day.v or results.day.volume depending on docs evolution
            const d = snap?.results?.day || {};
            dayVol = num(d?.v ?? d?.volume);
            // also refresh book from last_quote if present
            const lq = snap?.results?.last_quote;
            if (lq) updateMidFromQuoteRow(otk, lq);
          } catch {}

          for (const t of trows) {
            const price = num(t.price ?? t.p);
            const qty   = num(t.size  ?? t.s);
            if (!(price > 0 && qty > 0)) continue;

            const { side, side_src, at } = inferSideFromBook(otk, price);

            // priorVol = dayVol - qty at the moment of the print (approx; we don’t have per-tick dayVol)
            const priorVol = Number.isFinite(dayVol) ? Math.max(0, dayVol - qty) : 0;
            const { action, action_conf } = classifyOptionAction({
              qty, oi, priorVol, side, at
            });

            broadcast({
              type: "option_ts",
              symbol: otk,
              data: {
                id: `poly_${otk}_${t.id || t.sip_timestamp || Date.now()}`,
                ts: Date.now(),
                option: {
                  // You can parse poly option ticker if needed, but UI doesn’t require:
                  expiry: "", strike: 0, right: /C\d{5,}/.test(otk) ? "C" : "P"
                },
                qty,
                price,
                side, side_src,
                oi, priorVol,
                at,
                book,
                action, action_conf
              }
            });
          }
        }
      } catch (e) {
        console.warn("[polygon] trade loop error", e?.response?.status || e?.message);
        await sleep(1200);
      }
      await sleep(350); // small breather to respect rate limits
    }
  })();

  return () => { alive = false; };
}

