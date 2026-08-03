// Synthetic Signal Desk — always-on monitor
// Watches live Deriv prices, computes EMA/RSI/ATR signals, sends Telegram alerts
// with explicit entry, stop-loss and take-profit levels.

import WebSocket from 'ws';
import https from 'https';
import http from 'http';

// ---- Config (set these as environment variables in your host) ----
// Trim everything: pasting into a dashboard field very easily picks up a
// trailing newline or space, which the API then rejects as invalid.
const TELEGRAM_TOKEN = (process.env.TELEGRAM_TOKEN || '').trim();
const TELEGRAM_CHAT_ID = (process.env.TELEGRAM_CHAT_ID || '').trim();
// Deriv's public market-data socket. No token, no app_id, no authorize call.
// The legacy wss://ws.derivws.com/websockets/v3 endpoint now returns an empty
// instrument list unless authorized, which is why every symbol looked invalid.
const WS_URL = process.env.DERIV_WS_URL
  || 'wss://api.derivws.com/trading/v1/options/ws/public';
const CONFIDENCE_THRESHOLD = Number(process.env.CONFIDENCE_THRESHOLD || 70);
const GRANULARITY = Number(process.env.GRANULARITY_SECONDS || 900); // 900 = M15
const ALERT_COOLDOWN_MS = Number(process.env.ALERT_COOLDOWN_MINUTES || 15) * 60 * 1000;

// Risk levels, expressed as multiples of ATR(14) so they scale with each
// index's own volatility instead of assuming every symbol moves the same.
const SL_ATR_MULT = Number(process.env.SL_ATR_MULT || 1.5);
const TP_ATR_MULT = Number(process.env.TP_ATR_MULT || 3.0);

// Scoring sensitivity. These divisors were calibrated against simulated
// driftless random walks — which is essentially what a Deriv volatility index
// is — so that noise alone clears the 70% threshold on roughly 7% of candles,
// while a genuinely drifting series clears it on ~93%. Lowering them makes the
// bot far chattier; raising them makes it rarer and more selective.
const TREND_DIV = Number(process.env.TREND_DIV || 3.0);
const SLOPE_DIV = Number(process.env.SLOPE_DIV || 1.4);
const ENTRY_DIV = Number(process.env.ENTRY_DIV || 2.5);

const SYMBOLS = [
  { code: 'R_10', label: 'V10' },
  { code: 'R_25', label: 'V25' },
  { code: 'R_50', label: 'V50' },
  { code: 'R_75', label: 'V75' },
  { code: 'R_100', label: 'V100' },
  { code: 'BOOM500', label: 'B500' },
  { code: 'BOOM1000', label: 'B1000' },
  { code: 'CRASH500', label: 'C500' },
  { code: 'CRASH1000', label: 'C1000' },
  { code: 'stpRNG', label: 'STEP' },
];

if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) {
  console.error('Missing TELEGRAM_TOKEN or TELEGRAM_CHAT_ID environment variables.');
  process.exit(1);
}

// ---- Telegram sender ----
// markdown defaults to false: plain text can never fail to parse. Only the
// signal alerts opt in, because they are the only messages using *bold*.
// (Status messages mention things like DERIV_TOKEN, and a lone underscore
// makes Telegram reject the whole message with a 400.)
function sendTelegram(text, { markdown = false } = {}) {
  const body = { chat_id: TELEGRAM_CHAT_ID, text };
  if (markdown) body.parse_mode = 'Markdown';
  const payload = JSON.stringify(body);
  const req = https.request(
    {
      hostname: 'api.telegram.org',
      path: `/bot${TELEGRAM_TOKEN}/sendMessage`,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) },
    },
    (res) => {
      let body = '';
      res.on('data', (chunk) => (body += chunk));
      res.on('end', () => {
        if (res.statusCode !== 200) console.error('Telegram send failed:', res.statusCode, body);
      });
    }
  );
  req.on('error', (e) => console.error('Telegram request error:', e.message));
  req.write(payload);
  req.end();
}

// ---- Indicator math ----
function ema(values, period) {
  const k = 2 / (period + 1);
  let prev = values[0];
  const out = [];
  for (let i = 0; i < values.length; i++) {
    prev = i === 0 ? values[0] : values[i] * k + prev * (1 - k);
    out.push(prev);
  }
  return out;
}

function rsi(values, period = 14) {
  if (values.length < period + 1) return null;
  let gains = 0, losses = 0;
  for (let i = values.length - period; i < values.length; i++) {
    const diff = values[i] - values[i - 1];
    if (diff >= 0) gains += diff; else losses -= diff;
  }
  const avgGain = gains / period, avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

function atr(candles, period = 14) {
  if (candles.length < period + 1) return null;
  const trs = [];
  for (let i = 1; i < candles.length; i++) {
    const c = candles[i], p = candles[i - 1];
    trs.push(Math.max(c.high - c.low, Math.abs(c.high - p.close), Math.abs(c.low - p.close)));
  }
  const slice = trs.slice(-period);
  return slice.reduce((a, b) => a + b, 0) / slice.length;
}

const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

// Deriv sends prices as strings whose decimal count is the instrument's real
// precision. Rounding V10 to 2dp when it quotes 3dp would misstate the levels,
// so the precision is learned from the feed rather than hardcoded.
function decimalsOf(raw) {
  const s = String(raw);
  const dot = s.indexOf('.');
  return dot === -1 ? 0 : Math.min(6, s.length - dot - 1);
}

// ---- Confidence scoring ----
// Four independent components, each volatility-normalised by ATR so that a
// "strong" reading means the same thing on V10 as it does on V100. The old
// version added flat bonuses, which could only ever produce 50/60/65/75 —
// so with a threshold of 70 exactly one combination ever alerted.
function scoreSignal({ closes, ema20Series, ema50Series, rsiVal, atrVal, price }) {
  const ema20 = ema20Series.at(-1);
  const ema50 = ema50Series.at(-1);
  const up = ema20 > ema50;

  // Trend separation and slope carry most of the weight, because they are the
  // signal itself. RSI and entry quality only modulate it — if they were given
  // equal weight they would cancel the trend score out at exactly the moment
  // the trend is strongest (a hard trend is by definition overbought and
  // extended), which caps confidence below the alert threshold forever.

  // 1. Trend separation (0-35): how far apart the EMAs are, measured in ATRs.
  const gapAtr = Math.abs(ema20 - ema50) / atrVal;
  const trendScore = clamp(gapAtr / TREND_DIV, 0, 1) * 35;

  // 2. Slope (0-30): EMA20's move over the last 5 candles, in ATRs. A flat EMA
  //    that happens to sit above the slow one is a much weaker case than one
  //    actively pulling away.
  const ema20Prev = ema20Series.at(-6) ?? ema20Series[0];
  const slope = (ema20 - ema20Prev) / atrVal;
  const slopeAligned = up ? slope : -slope;
  const slopeScore = clamp(slopeAligned / SLOPE_DIV, 0, 1) * 30;

  // 3. RSI agreement (0-20): does momentum confirm the EMA trend, and is it
  //    running out of room? Deep overbought on a long is a worse entry than a
  //    fresh one, so exhaustion decays this component — but only down to 35%,
  //    never to zero, since a strong trend should still be tradeable.
  const rsiAligned = up ? rsiVal - 50 : 50 - rsiVal;
  let rsiScore = clamp(rsiAligned / 18, 0, 1) * 20;
  if (up && rsiVal > 75) rsiScore *= Math.max(0.35, 1 - (rsiVal - 75) / 20);
  if (!up && rsiVal < 25) rsiScore *= Math.max(0.35, 1 - (25 - rsiVal) / 20);

  // 4. Entry quality (0-15): price close to EMA20 means you are joining the
  //    trend near its mean rather than chasing an extended candle.
  const distAtr = Math.abs(price - ema20) / atrVal;
  const entryScore = clamp(1 - distAtr / ENTRY_DIV, 0, 1) * 15;

  const raw = trendScore + rsiScore + slopeScore + entryScore; // 0-100
  const confidence = Math.round(30 + (raw / 100) * 65);        // 30-95

  return {
    confidence: clamp(confidence, 30, 95),
    direction: up ? 'BUY' : 'SELL',
    parts: {
      trend: Math.round(trendScore),
      rsi: Math.round(rsiScore),
      slope: Math.round(slopeScore),
      entry: Math.round(entryScore),
    },
  };
}

// ---- Risk levels ----
function levelsFor(direction, price, atrVal) {
  const risk = SL_ATR_MULT * atrVal;
  const reward = TP_ATR_MULT * atrVal;
  return direction === 'BUY'
    ? { entry: price, sl: price - risk, tp: price + reward, risk, reward }
    : { entry: price, sl: price + risk, tp: price - reward, risk, reward };
}

const rrLabel = (() => {
  const rr = TP_ATR_MULT / SL_ATR_MULT;
  return `1:${Number.isInteger(rr) ? rr : rr.toFixed(1)}`;
})();

// ---- Per-symbol state ----
// Keyed by the symbol code Deriv actually accepts, which is resolved at runtime.
const state = {};
const labels = {};

function evaluateSignal(code, label) {
  const st = state[code];
  if (st.candles.length < 55) return;
  const closes = st.candles.map((c) => c.close);
  const ema20Series = ema(closes, 20);
  const ema50Series = ema(closes, 50);
  const rsiVal = rsi(closes, 14);
  const atrVal = atr(st.candles, 14);
  const price = closes.at(-1);

  if (rsiVal === null || !Number.isFinite(price)) return;
  // Every level below is an ATR multiple, so without a usable ATR there is no
  // signal to send — better to stay quiet than to publish a stop of zero.
  if (!atrVal || !Number.isFinite(atrVal) || atrVal <= 0) return;

  const { confidence, direction, parts } = scoreSignal({
    closes, ema20Series, ema50Series, rsiVal, atrVal, price,
  });

  const strong = confidence >= CONFIDENCE_THRESHOLD;
  const crossedUp = strong && st.lastConfidence < CONFIDENCE_THRESHOLD;
  const flipped = strong && st.lastDirection !== null && st.lastDirection !== direction;
  const cooledDown = Date.now() - st.lastAlertAt > ALERT_COOLDOWN_MS;

  if ((crossedUp || flipped) && cooledDown) {
    const { entry, sl, tp, risk, reward } = levelsFor(direction, price, atrVal);
    const d = st.decimals ?? 2;
    const arrow = direction === 'BUY' ? '\u{1F7E2}' : '\u{1F534}';

    sendTelegram(
      `${arrow} *${direction} ${label} NOW*\n\n` +
      `Entry  ${entry.toFixed(d)}\n` +
      `SL     ${sl.toFixed(d)}  (${direction === 'BUY' ? '-' : '+'}${risk.toFixed(d)})\n` +
      `TP     ${tp.toFixed(d)}  (${direction === 'BUY' ? '+' : '-'}${reward.toFixed(d)})\n` +
      `R:R    ${rrLabel}\n\n` +
      `*Confidence ${confidence}%*\n` +
      `RSI(14) ${rsiVal.toFixed(1)} · ATR(14) ${atrVal.toFixed(d)}`,
      { markdown: true }
    );
    console.log(
      `[ALERT] ${label} ${direction} @ ${confidence}% ` +
      `entry=${entry.toFixed(d)} sl=${sl.toFixed(d)} tp=${tp.toFixed(d)} ` +
      `parts=${JSON.stringify(parts)}`
    );
    st.lastAlertAt = Date.now();
    st.lastSignal = { direction, confidence, entry, sl, tp, at: Date.now() };
  }

  st.lastDirection = direction;
  st.lastConfidence = confidence;
}

// ---- Deriv WebSocket connection with auto-reconnect ----
let ws;
let reconnectDelay = 3000;
let activeSymbols = []; // resolved from Deriv after authorize

// Ask Deriv what actually exists, rather than assuming symbol codes.
function requestSymbols() {
  ws.send(JSON.stringify({ active_symbols: 'brief' }));
}

// Match our watchlist against what Deriv really offers. Falls back to
// display-name matching when a hardcoded code has been renamed.
// Note: this endpoint uses underlying_symbol / underlying_symbol_name,
// not the symbol / display_name of the legacy API.
function resolveWatchlist(available) {
  const byCode = new Map(available.map((s) => [s.underlying_symbol, s]));
  const resolved = [];
  const missing = [];

  for (const want of SYMBOLS) {
    if (byCode.has(want.code)) {
      resolved.push({ code: want.code, label: want.label });
      continue;
    }
    // e.g. "BOOM1000" -> look for a display name containing "boom" and "1000"
    const parts = want.code.toLowerCase().match(/[a-z]+|\d+/g) || [];
    const hit = available.find((s) => {
      const name = (s.underlying_symbol_name || '').toLowerCase().replace(/\s+/g, '');
      return parts.every((p) => name.includes(p));
    });
    if (hit) {
      console.log(`Remapped ${want.code} -> ${hit.underlying_symbol} ("${hit.underlying_symbol_name}")`);
      resolved.push({ code: hit.underlying_symbol, label: want.label });
    } else {
      missing.push(want.code);
    }
  }

  if (missing.length) console.error('No Deriv symbol found for:', missing.join(', '));
  return resolved;
}

function subscribe(list) {
  list.forEach((s) => {
    if (!state[s.code]) {
      state[s.code] = {
        candles: [], lastDirection: null, lastConfidence: 0,
        lastAlertAt: 0, lastSignal: null, decimals: null,
      };
    }
    labels[s.code] = s.label;
    ws.send(JSON.stringify({
      ticks_history: s.code,
      style: 'candles',
      granularity: GRANULARITY,
      count: 150,
      end: 'latest',
      subscribe: 1,
    }));
  });
}

function connect() {
  ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    console.log('Connected to Deriv public feed.');
    reconnectDelay = 3000;
    requestSymbols();
  });

  ws.on('message', (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch (e) {
      console.error('Bad message from Deriv, ignoring:', e.message);
      return;
    }
    if (data.error) {
      console.error('Deriv error:', data.error.code, data.error.message,
        '| requested symbol:', data.echo_req?.ticks_history);
      return;
    }

    if (data.active_symbols) {
      const all = data.active_symbols;
      const synth = all.filter((s) => s.market === 'synthetic_index');
      console.log(`Deriv returned ${all.length} symbols (${synth.length} synthetic).`);
      if (!all.length) {
        console.error('Empty symbol list from the public endpoint — unexpected.');
        sendTelegram('Deriv returned no instruments. The feed endpoint may have changed.');
        return;
      }
      activeSymbols = resolveWatchlist(synth);
      if (!activeSymbols.length) {
        console.error('None of the watchlist symbols exist on Deriv. Nothing to monitor.');
        sendTelegram('None of the watchlist symbols matched Deriv instruments.');
        return;
      }
      console.log('Monitoring:', activeSymbols.map((s) => s.label).join(', '));
      sendTelegram(
        'Monitor live — watching ' + activeSymbols.map((s) => s.label).join(', ') +
        `\nAlerting above ${CONFIDENCE_THRESHOLD}% confidence, ` +
        `SL ${SL_ATR_MULT}x ATR / TP ${TP_ATR_MULT}x ATR (${rrLabel}).`
      );
      subscribe(activeSymbols);
      return;
    }

    const reqSymbol = data.echo_req?.ticks_history;
    if (data.msg_type === 'candles' && reqSymbol && state[reqSymbol]) {
      const st = state[reqSymbol];
      if (st.decimals === null && data.candles.length) {
        st.decimals = Math.max(
          decimalsOf(data.candles.at(-1).close),
          decimalsOf(data.candles.at(-1).open)
        );
      }
      st.candles = data.candles.map((c) => ({
        time: c.epoch, open: +c.open, high: +c.high, low: +c.low, close: +c.close,
      }));
      evaluateSignal(reqSymbol, labels[reqSymbol] || reqSymbol);
    }
    if (data.msg_type === 'ohlc' && data.ohlc) {
      const symCode = data.ohlc.symbol;
      const st = state[symCode];
      if (!st) return;
      if (st.decimals === null) st.decimals = decimalsOf(data.ohlc.close);
      const point = {
        time: data.ohlc.open_time, open: +data.ohlc.open, high: +data.ohlc.high,
        low: +data.ohlc.low, close: +data.ohlc.close,
      };
      const last = st.candles.at(-1);
      if (last && last.time === point.time) st.candles[st.candles.length - 1] = point;
      else st.candles.push(point);
      if (st.candles.length > 150) st.candles.shift();
      evaluateSignal(symCode, labels[symCode] || symCode);
    }
  });

  ws.on('close', () => {
    console.log(`Disconnected. Reconnecting in ${reconnectDelay / 1000}s...`);
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 1.5, 60000);
  });

  ws.on('error', (e) => {
    console.error('WebSocket error:', e.message);
    ws.close();
  });
}

// Don't let one unexpected error kill the whole monitor silently.
process.on('unhandledRejection', (e) => console.error('Unhandled rejection:', e));
process.on('uncaughtException', (e) => console.error('Uncaught exception:', e));

// ---- Optional health endpoint ----
// Only binds if PORT is set. Render Background Workers don't set PORT, so this
// stays off there. Render/Railway Web Services do set it, which lets the same
// codebase run as a free Web Service that satisfies the "must bind a port" check.
if (process.env.PORT) {
  http.createServer((req, res) => {
    const ready = Object.values(state).filter((s) => s.candles.length >= 55).length;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      socket: ws?.readyState === 1 ? 'connected' : 'reconnecting',
      monitoring: activeSymbols.map((s) => s.label),
      symbolsReady: `${ready}/${activeSymbols.length || SYMBOLS.length}`,
      threshold: CONFIDENCE_THRESHOLD,
      risk: { slAtrMult: SL_ATR_MULT, tpAtrMult: TP_ATR_MULT, rr: rrLabel },
      lastSignals: Object.fromEntries(
        Object.entries(state)
          .filter(([, s]) => s.lastSignal)
          .map(([code, s]) => [labels[code] || code, s.lastSignal])
      ),
      uptimeSeconds: Math.round(process.uptime()),
    }));
  }).listen(process.env.PORT, () => console.log(`Health endpoint on :${process.env.PORT}`));
}

sendTelegram('Synthetic Signal Desk starting up...');
connect();
