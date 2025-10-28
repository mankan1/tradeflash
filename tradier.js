export function normalizeTradier(trades = [], now = Date.now()) {
  return trades.map((t) => {
    const occ = t.symbol || t.occ_symbol;
    const symbol = (t.underlying || (occ?.split(" ")[0]) || "").toUpperCase();
    const price = +t.price || 0;
    const size = +t.size || +t.volume || 0;
    const ts = new Date(t.timestamp || now).getTime();
    const id = `tradier:${ts}:${occ}:${price}:${size}`;

    const side = t.side === "buy" || t.side === 1
      ? "BUY"
      : t.side === "sell" || t.side === -1
      ? "SELL"
      : "UNKNOWN";

    return {
      id,
      provider: "tradier",
      ts,
      symbol,
      occ,
      expiry: t.expiration,
      strike: +t.strike || undefined,
      callPut: t.option_type?.toUpperCase().startsWith("C") ? "C" :
               t.option_type?.toUpperCase().startsWith("P") ? "P" : undefined,
      price,
      size,
      notional: price * 100 * size || undefined,
      side,
      kind: detectKind(t.conditions),
      exchange: t.exchange,
      iv: +t.iv || undefined,
      delta: +t.delta || undefined,
      oi: +t.open_interest || undefined,
    };
  });
}

export function detectKind(conds) {
  const s = Array.isArray(conds) ? conds.join(",").toUpperCase() : String(conds || "").toUpperCase();
  if (s.includes("SWEEP")) return "SWEEP";
  if (s.includes("BLOCK")) return "BLOCK";
  return "UNKNOWN";
}

//module.exports = { normalizeTradier };
