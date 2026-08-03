// Synthetic Signal Desk — always-on monitor
// Watches live Deriv prices, computes EMA/RSI/ATR signals, sends Telegram alerts.

import WebSocket from 'ws';
import https from 'https';
import http from 'http';

// ---- Config (set these as environment variables in your host) ----
// Trim everything: pasting into a dashboard field very easily picks up a
// trailing newline or space, which the API then rejects as invalid.
const TELEGRAM_TOKEN = (process.env.TELEGRAM_TOKEN || '').trim();
const TELEGRAM_CHAT_ID = (process.env.TELEGRAM_CHAT_ID || '').trim();
// Deriv now returns an empty instrument list to unauthenticated connections,
// so a read-scope API token is required for any market data.
const DERIV_TOKEN = (process.env.DERIV_TOKEN || '').trim();
const APP_ID = process.env.DERIV_APP_ID || '1089';
const CONFIDENCE_THRESHOLD = Number(process.env.CONFIDENCE_THRESHOLD || 70);
const GRANULARITY = Number(process.env.GRANULARITY_SECONDS || 900); // 900 = M15
const ALERT_COOLDOWN_MS = Number(process.env.ALERT_COOLDOWN_MINUTES || 15) * 60 * 1000;

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

// ---- Per-symbol state ----
// Keyed by the symbol code Deriv actually accepts, which is resolved at runtime.
const state = {};
const labels = {};

function evaluateSignal(code, label) {
  const st = state[code];
  if (st.candles.length < 55) return;
  const closes = st.candles.map((c) => c.close);
  const ema20 = ema(closes, 20).at(-1);
  const ema50 = ema(closes, 50).at(-1);
  const rsiVal = rsi(closes, 14);
  const atrVal = atr(st.candles, 14);
  const price = closes.at(-1);
  if (rsiVal === null || !Number.isFinite(price)) return;
  const trendUp = ema20 > ema50;

  let confidence = 50;
  if (trendUp && rsiVal < 70) confidence += 15;
  if (!trendUp && rsiVal > 30) confidence += 15;
  if (Math.abs(ema20 - ema50) / price > 0.001) confidence += 10;
  confidence = Math.min(95, Math.max(30, Math.round(confidence)));
  const direction = trendUp ? 'BUY' : 'SELL';

  const crossedIn = confidence >= CONFIDENCE_THRESHOLD && st.lastConfidence < CONFIDENCE_THRESHOLD;
  const directionFlip = st.lastDirection && st.lastDirection !== direction && confidence >= CONFIDENCE_THRESHOLD;
  const cooledDown = Date.now() - st.lastAlertAt > ALERT_COOLDOWN_MS;

  if ((directionFlip || (crossedIn && st.lastDirection === direction)) && cooledDown) {
    sendTelegram(
      `*${direction}* signal — ${label}\nConfidence: ${confidence}%\nPrice: ${price.toFixed(2)}\nRSI(14): ${rsiVal.toFixed(1)}\nATR(14): ${atrVal ? atrVal.toFixed(2) : '—'}`,
      { markdown: true }
    );
    console.log(`[ALERT] ${label} ${direction} @ ${confidence}%`);
    st.lastAlertAt = Date.now();
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
function resolveWatchlist(available) {
  const byCode = new Map(available.map((s) => [s.symbol, s]));
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
      const name = (s.display_name || '').toLowerCase().replace(/\s+/g, '');
      return parts.every((p) => name.includes(p));
    });
    if (hit) {
      console.log(`Remapped ${want.code} -> ${hit.symbol} ("${hit.display_name}")`);
      resolved.push({ code: hit.symbol, label: want.label });
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
      state[s.code] = { candles: [], lastDirection: null, lastConfidence: 0, lastAlertAt: 0 };
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
  ws = new WebSocket(`wss://ws.derivws.com/websockets/v3?app_id=${APP_ID}`);

  ws.on('open', () => {
    console.log('Connected to Deriv feed.');
    reconnectDelay = 3000;
    if (DERIV_TOKEN) {
      ws.send(JSON.stringify({ authorize: DERIV_TOKEN }));
    } else {
      console.error(
        'No DERIV_TOKEN set. Deriv returns an empty symbol list to unauthenticated ' +
        'connections, so no signals can be produced. Set DERIV_TOKEN and redeploy.'
      );
      requestSymbols(); // still request, so the empty result is visible in logs
    }
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
      if (data.error.code === 'InvalidToken' || data.error.code === 'AuthorizationRequired') {
        console.error(
          `Token length seen by the app: ${DERIV_TOKEN.length} chars. ` +
          'If that is 0, the variable is not set. If it looks short or long, the paste was truncated.'
        );
        sendTelegram('Deriv rejected the API token. Check the DERIV_TOKEN value in Render and redeploy.');
      }
      return;
    }

    if (data.msg_type === 'authorize') {
      console.log('Authorized with Deriv as', data.authorize?.loginid || '(unknown account)');
      requestSymbols();
      return;
    }

    if (data.msg_type === 'active_symbols') {
      const all = data.active_symbols || [];
      const synth = all.filter((s) => s.market === 'synthetic_index');
      console.log(`Deriv returned ${all.length} symbols (${synth.length} synthetic).`);
      if (!all.length) {
        console.error(
          'Empty symbol list. Deriv requires an authorized token with read access ' +
          'before it will return instruments.'
        );
        sendTelegram('⚠️ Deriv returned no instruments — DERIV_TOKEN is missing or lacks access.');
        return;
      }
      synth.forEach((s) => console.log('  available:', s.symbol, '|', s.display_name));
      activeSymbols = resolveWatchlist(synth);
      if (!activeSymbols.length) {
        console.error('None of the watchlist symbols exist on Deriv. Nothing to monitor.');
        sendTelegram('⚠️ None of the watchlist symbols matched Deriv instruments.');
        return;
      }
      console.log('Monitoring:', activeSymbols.map((s) => s.label).join(', '));
      subscribe(activeSymbols);
      return;
    }

    const reqSymbol = data.echo_req?.ticks_history;
    if (data.msg_type === 'candles' && reqSymbol && state[reqSymbol]) {
      state[reqSymbol].candles = data.candles.map((c) => ({
        time: c.epoch, open: +c.open, high: +c.high, low: +c.low, close: +c.close,
      }));
      evaluateSignal(reqSymbol, labels[reqSymbol] || reqSymbol);
    }
    if (data.msg_type === 'ohlc' && data.ohlc) {
      const symCode = data.ohlc.symbol;
      const st = state[symCode];
      if (!st) return;
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
      authorized: Boolean(DERIV_TOKEN),
      monitoring: activeSymbols.map((s) => s.label),
      symbolsReady: `${ready}/${activeSymbols.length || SYMBOLS.length}`,
      uptimeSeconds: Math.round(process.uptime()),
    }));
  }).listen(process.env.PORT, () => console.log(`Health endpoint on :${process.env.PORT}`));
}

sendTelegram('✅ Synthetic Signal Desk monitor started — watching ' + SYMBOLS.map((s) => s.label).join(', '));
connect();
