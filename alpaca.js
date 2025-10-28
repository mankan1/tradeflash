export function normalizeAlpaca(trades = [], now = Date.now()) {
  return trades.map((t) => {
    const occ = t.symbol;
    const symbol = (t.underlying_symbol || (occ?.split(" ")[0]) || "").toUpperCase();
    const price = +t.price || 0;
    const size = +t.size || +t.volume || 0;
    const ts = new Date(t.ts || now).getTime();
    const id = `alpaca:${ts}:${occ}:${price}:${size}`;

    const side = t.side === "buy" || t.side === 1
      ? "BUY"
      : t.side === "sell" || t.side === -1
      ? "SELL"
      : "UNKNOWN";

    return {
      id,
      provider: "alpaca",
      ts,
      symbol,
      occ,
      expiry: t.expiration,
      strike: +t.strike || undefined,
      callPut: t.type?.toLowerCase() === "call" ? "C" :
               t.type?.toLowerCase() === "put" ? "P" : undefined,
      price,
      size,
      notional: price * 100 * size || undefined,
      side,
      kind: t.is_sweep ? "SWEEP" : (t.is_block ? "BLOCK" : "UNKNOWN"),
      exchange: t.exchange,
      iv: +t.iv || undefined,
      delta: +t.delta || undefined,
      oi: +t.oi || undefined,
    };
  });
}

//module.exports = { normalizeAlpaca };
