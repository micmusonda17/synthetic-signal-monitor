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
// 74 was chosen by simulation: it yields roughly 6 signals a day across the
// full 10-symbol watchlist. Drop to 72 for ~8/day, raise to 78 for ~3/day.
const CONFIDENCE_THRESHOLD = Number(process.env.CONFIDENCE_THRESHOLD || 74);
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

// A trade that has gone nowhere is not a trade, it is exposure. After this many
// candles the monitor stops waiting and tells you to close it. 24 M15 candles
// is 6 hours, which in simulation covers about 90% of trades that resolve.
const TIME_STOP_BARS = Number(process.env.TIME_STOP_BARS || 24);

// Hold-time expectations, in candles. These start as simulation-derived
// estimates and are replaced by this bot's own measured results as soon as it
// has enough closed trades to say something meaningful.
const HOLD_FALLBACK = { p25: 3, median: 6, p75: 10 };
const MIN_HOLD_SAMPLES = Number(process.env.MIN_HOLD_SAMPLES || 20);

// How much real history to pull per symbol on startup. Deriv caps ticks_history
// at 5000 candles; 2000 M15 candles is about three weeks per instrument, which
// is enough to calibrate against without slowing the boot noticeably.
const HISTORY_BARS = Math.min(5000, Number(process.env.HISTORY_BARS || 2000));

// ---- Boom/Crash spike analysis ----
// Boom drifts down and spikes up; Crash drifts up and spikes down, on average
// every 500 or 1000 ticks. That published average is the only piece of genuine
// engineered structure in the whole product range, so it is the only place a
// real edge could live. Whether it can be exploited depends entirely on the
// shape of the gap distribution, which is what SPIKE_BATCHES of tick history
// is collected to measure. 12 batches of 5000 ticks is roughly 16 hours.
const SPIKE_META = {
  B500: { nominal: 500, dir: +1 },
  B1000: { nominal: 1000, dir: +1 },
  C500: { nominal: 500, dir: -1 },
  C1000: { nominal: 1000, dir: -1 },
};
const SPIKE_BATCHES = Number(process.env.SPIKE_BATCHES || 12);
const SPIKE_MULT = Number(process.env.SPIKE_MULT || 8);

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

// ---- Hold-time guidance ----
// Candle counts are meaningless to read at a glance, so everything the user
// sees is expressed in wall-clock time derived from the configured granularity.
function humanDuration(bars) {
  const mins = Math.round((bars * GRANULARITY) / 60);
  if (mins < 60) return `${mins}m`;
  const h = mins / 60;
  return h < 10 ? `${h.toFixed(1).replace(/\.0$/, '')}h` : `${Math.round(h)}h`;
}

// Every closed trade feeds back into the hold estimate, so the guidance stops
// being a simulation artefact and starts describing this specific bot on these
// specific instruments. Timed-out trades are excluded: they never resolved, so
// including them would bias the "how long until TP or SL" answer upward.
const outcomes = { tp: 0, sl: 0, timeout: 0, resolvedBars: [] };

function percentiles(arr) {
  const s = [...arr].sort((a, b) => a - b);
  const q = (p) => s[Math.min(s.length - 1, Math.floor(s.length * p))];
  return { p25: q(0.25), median: q(0.5), p75: q(0.75) };
}

// Preference order: this bot's own closed trades, then the startup backtest on
// real Deriv history, then the simulated fallback. Each step is closer to what
// the user will actually experience than the one after it.
function holdGuidance() {
  if (outcomes.resolvedBars.length >= MIN_HOLD_SAMPLES) {
    return { ...percentiles(outcomes.resolvedBars), source: 'live', n: outcomes.resolvedBars.length };
  }
  if (backtest.bars.length >= MIN_HOLD_SAMPLES) {
    return { ...percentiles(backtest.bars), source: 'backtest', n: backtest.bars.length };
  }
  return { ...HOLD_FALLBACK, source: 'simulated', n: 0 };
}

// ---- Startup backtest on real Deriv history ----
// A stop that keeps getting hit is usually too tight for the instrument's noise
// rather than a sign the direction was wrong, so the backtest does not just
// score the configured levels — it replays several stop/target pairs over the
// same signals and reports which one actually held up.
const RISK_VARIANTS = [
  { sl: 1.0, tp: 2.0 },
  { sl: 1.5, tp: 3.0 },
  { sl: 2.0, tp: 3.0 },
  { sl: 2.0, tp: 4.0 },
  { sl: 2.5, tp: 5.0 },
  { sl: 3.0, tp: 3.0 },
];
const variantKey = (v) => `${v.sl}/${v.tp}`;
const ACTIVE_VARIANT = variantKey({ sl: SL_ATR_MULT, tp: TP_ATR_MULT });

function blankVariant() {
  return { signals: 0, tp: 0, sl: 0, timeout: 0, sumR: 0, bars: [] };
}

const backtest = {
  done: 0, days: 0, reported: false,
  variants: Object.fromEntries(RISK_VARIANTS.map((v) => [variantKey(v), blankVariant()])),
  get active() { return this.variants[ACTIVE_VARIANT] || blankVariant(); },
  get signals() { return this.active.signals; },
  get tp() { return this.active.tp; },
  get sl() { return this.active.sl; },
  get timeout() { return this.active.timeout; },
  get bars() { return this.active.bars; },
};

// The configured levels may not be one of the presets, so make sure they are
// always measured — otherwise the summary would report on settings the user
// is not actually running.
if (!backtest.variants[ACTIVE_VARIANT]) {
  RISK_VARIANTS.push({ sl: SL_ATR_MULT, tp: TP_ATR_MULT });
  backtest.variants[ACTIVE_VARIANT] = blankVariant();
}

// Replays exactly the live gating rules over historical candles: same score,
// same threshold cross / flip trigger, same cooldown, same one-position rule,
// same time stop. If this disagrees with live behaviour later, the rules here
// have drifted from evaluateSignal and that is a bug, not a market change.
function backtestSymbol(code, label, candles) {
  if (candles.length < 200) return;
  const cooldownBars = Math.max(1, Math.round(ALERT_COOLDOWN_MS / (GRANULARITY * 1000)));

  // Scoring is by far the expensive part and does not depend on stop placement,
  // so it runs once per bar and every risk variant reuses the result.
  const scores = new Array(candles.length).fill(null);
  for (let i = 60; i < candles.length; i++) {
    const win = candles.slice(Math.max(0, i - 150), i);
    if (win.length < 55) continue;
    const closes = win.map((c) => c.close);
    const atrVal = atr(win, 14);
    const rsiVal = rsi(closes, 14);
    const price = closes.at(-1);
    if (!atrVal || atrVal <= 0 || rsiVal === null || !Number.isFinite(price)) continue;
    const { confidence, direction } = scoreSignal({
      closes, ema20Series: ema(closes, 20), ema50Series: ema(closes, 50),
      rsiVal, atrVal, price,
    });
    scores[i] = { confidence, direction, atrVal, price };
  }

  for (const v of RISK_VARIANTS) {
    const acc = backtest.variants[variantKey(v)];
    let lastConf = 0, lastDir = null, lastAlertBar = -1e9, open = null;

    for (let i = 60; i < candles.length; i++) {
      const bar = candles[i];

      if (open) {
        const held = i - open.i;
        const long = open.dir === 'BUY';
        const hitSl = long ? bar.low <= open.sl : bar.high >= open.sl;
        const hitTp = long ? bar.high >= open.tp : bar.low <= open.tp;
        let out = null;
        // Stop checked first: if one candle spans both levels there is no way to
        // know which traded first, and assuming the loss is the honest default.
        if (hitSl) out = 'SL';
        else if (hitTp) out = 'TP';
        else if (held >= TIME_STOP_BARS) out = 'TIME';
        if (out) {
          if (out === 'TP') { acc.tp++; acc.sumR += v.tp / v.sl; }
          else if (out === 'SL') { acc.sl++; acc.sumR -= 1; }
          else {
            acc.timeout++;
            // A timed-out trade is closed at market, so credit its real result
            // rather than pretending it was flat.
            const move = long ? bar.close - open.entry : open.entry - bar.close;
            acc.sumR += move / open.risk;
          }
          if (out !== 'TIME') acc.bars.push(held);
          open = null;
          lastAlertBar = i;
        }
        continue;
      }

      const sc = scores[i];
      if (!sc) continue;
      const strong = sc.confidence >= CONFIDENCE_THRESHOLD;
      const crossedUp = strong && lastConf < CONFIDENCE_THRESHOLD;
      const flipped = strong && lastDir !== null && lastDir !== sc.direction;
      if ((crossedUp || flipped) && (i - lastAlertBar) > cooldownBars) {
        const long = sc.direction === 'BUY';
        const risk = v.sl * sc.atrVal;
        const reward = v.tp * sc.atrVal;
        open = {
          i, dir: sc.direction, entry: sc.price, risk,
          sl: long ? sc.price - risk : sc.price + risk,
          tp: long ? sc.price + reward : sc.price - reward,
        };
        acc.signals++;
      }
      lastConf = sc.confidence;
      lastDir = sc.direction;
    }
  }

  const spanDays = (candles.at(-1).time - candles[0].time) / 86400;
  backtest.done += 1;
  backtest.days = Math.max(backtest.days, spanDays);
  const a = backtest.active;
  console.log(
    `[BACKTEST] ${label}: ${spanDays.toFixed(1)}d, active ${ACTIVE_VARIANT} -> ` +
    `${a.signals} signals TP=${a.tp} SL=${a.sl} timeout=${a.timeout}`
  );
}

// Reported once, after every symbol has been replayed, so the user gets a single
// honest summary of what these settings would have produced recently.
function variantSummary(key) {
  const a = backtest.variants[key];
  const closed = a.tp + a.sl + a.timeout;
  if (!closed) return null;
  return {
    key, closed,
    winRate: (100 * a.tp) / closed,
    expectancy: a.sumR / closed,
    perDay: a.signals / Math.max(backtest.days, 1),
    hold: percentiles(a.bars.length ? a.bars : [HOLD_FALLBACK.median]),
  };
}

function reportBacktest() {
  if (backtest.reported || !activeSymbols.length || backtest.done < activeSymbols.length) return;
  backtest.reported = true;

  const active = variantSummary(ACTIVE_VARIANT);
  if (!active) return;
  const all = Object.keys(backtest.variants).map(variantSummary).filter(Boolean);
  const best = all.reduce((a, b) => (b.expectancy > a.expectancy ? b : a));

  const rows = all
    .sort((a, b) => b.expectancy - a.expectancy)
    .map((s) => {
      const mark = s.key === ACTIVE_VARIANT ? ' (current)' : '';
      const e = `${s.expectancy >= 0 ? '+' : ''}${s.expectancy.toFixed(2)}R`;
      return `SL ${s.key.split('/')[0]}x TP ${s.key.split('/')[1]}x — ` +
        `win ${s.winRate.toFixed(0)}%, ${e}/trade${mark}`;
    })
    .join('\n');

  const advice = best.key === ACTIVE_VARIANT
    ? 'Your current levels came out best of those tested.'
    : `Better on this data: SL ${best.key.split('/')[0]}x TP ${best.key.split('/')[1]}x ` +
      `(${best.expectancy >= 0 ? '+' : ''}${best.expectancy.toFixed(2)}R vs ` +
      `${active.expectancy >= 0 ? '+' : ''}${active.expectancy.toFixed(2)}R). ` +
      `Set SL ATR MULT=${best.key.split('/')[0]} and TP ATR MULT=${best.key.split('/')[1]} in Render to switch.`;

  sendTelegram(
    `Backtest — ${backtest.days.toFixed(0)} days of real Deriv candles, ` +
    `${activeSymbols.length} symbols, threshold ${CONFIDENCE_THRESHOLD}%\n\n` +
    `Current settings (SL ${SL_ATR_MULT}x / TP ${TP_ATR_MULT}x):\n` +
    `Signals ${active.perDay.toFixed(1)}/day · win ${active.winRate.toFixed(0)}% · ` +
    `${active.expectancy >= 0 ? '+' : ''}${active.expectancy.toFixed(2)}R per trade\n` +
    `Typical hold ${humanDuration(active.hold.median)} ` +
    `(${humanDuration(active.hold.p25)} to ${humanDuration(active.hold.p75)})\n\n` +
    `Stop and target comparison:\n${rows}\n\n${advice}`
  );

  console.log(`[BACKTEST] active ${ACTIVE_VARIANT}: ${active.perDay.toFixed(2)}/day, ` +
    `win ${active.winRate.toFixed(1)}%, exp ${active.expectancy.toFixed(3)}R | best ${best.key}`);
}

// ---- Boom/Crash spike lab ----
// The "count ticks since the last spike" idea rests on one assumption: that
// waiting makes a spike more likely. That is only true if the gaps between
// spikes are NOT geometrically distributed. A geometric distribution is
// memoryless - if Deriv rolls a 1-in-500 die on every tick, then after 900 quiet
// ticks the chance of a spike on the next tick is still exactly 1 in 500, and
// counting tells you precisely nothing. Every spike bot on the market assumes
// otherwise without checking. This measures it.

function median(xs) {
  if (!xs.length) return 0;
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

// A spike is a single-tick move many times larger than this instrument's normal
// tick-to-tick movement. The threshold is derived from the data so it adapts to
// each symbol instead of being hardcoded.
function detectSpikes(quotes, direction, mult = SPIKE_MULT) {
  const diffs = [];
  for (let i = 1; i < quotes.length; i++) diffs.push(quotes[i] - quotes[i - 1]);
  const typical = median(diffs.map(Math.abs).filter((d) => d > 0));
  const threshold = typical * mult;
  const indices = [];
  for (let i = 0; i < diffs.length; i++) {
    const d = diffs[i];
    if (direction > 0 ? d > threshold : d < -threshold) indices.push(i + 1);
  }
  return { indices, typicalMove: typical, threshold };
}

function gapsBetween(indices) {
  const gaps = [];
  for (let i = 1; i < indices.length; i++) gaps.push(indices[i] - indices[i - 1]);
  return gaps;
}

// Discrete hazard: of the gaps that survived to the start of a bucket, what
// fraction ended inside it? Expressed per tick so buckets stay comparable.
function hazardCurve(gaps, bucketSize = 100) {
  if (!gaps.length) return [];
  const top = Math.max(...gaps);
  const out = [];
  for (let lo = 0; lo < top; lo += bucketSize) {
    const hi = lo + bucketSize;
    const atRisk = gaps.filter((g) => g > lo).length;
    const events = gaps.filter((g) => g > lo && g <= hi).length;
    if (atRisk < 15) break; // too few survivors for the estimate to mean anything
    out.push({ from: lo, to: hi, atRisk, events, perTick: events / atRisk / bucketSize });
  }
  return out;
}

function memorylessTest(gaps, bucketSize = 100) {
  const curve = hazardCurve(gaps, bucketSize);
  if (curve.length < 4) return { usable: false, reason: 'not enough spikes yet' };
  const half = Math.floor(curve.length / 2);
  const w = (bs) => {
    const ev = bs.reduce((a, b) => a + b.events, 0);
    const risk = bs.reduce((a, b) => a + b.atRisk * bucketSize, 0);
    return risk ? ev / risk : 0;
  };
  const h1 = w(curve.slice(0, half));
  const h2 = w(curve.slice(half));
  // An early hazard of exactly zero means spikes never occur before a certain
  // age - a guaranteed quiet period, which is the strongest edge available, not
  // the absence of one. Dividing by it would report the opposite.
  const ratio = h1 > 0 ? h2 / h1 : (h2 > 0 ? Infinity : 0);
  // A memoryless process has standard deviation equal to its mean, so this sits
  // at 1.0. Well below 1.0 means gaps cluster around a typical length.
  const mean = gaps.reduce((a, b) => a + b, 0) / gaps.length;
  const sd = Math.sqrt(gaps.reduce((a, b) => a + (b - mean) ** 2, 0) / gaps.length);
  const cv = mean ? sd / mean : 0;
  // Both statistics must agree before this claims an edge. The hazard ratio
  // alone is unstable, because the late buckets are computed from whatever few
  // long gaps survived, and on a genuinely memoryless series it produces false
  // positives roughly one run in six. Claiming an edge that is not there is the
  // expensive mistake here, so the coefficient of variation - which uses every
  // gap rather than the tail - has to back it up.
  const enough = gaps.length >= 30;
  const verdict = !enough ? 'INSUFFICIENT'
    : (cv < 0.75 && ratio > 1.2) ? 'EDGE'
    : (cv < 0.9 && ratio > 1.1) ? 'WEAK' : 'NO EDGE';
  return { usable: true, curve, earlyHazard: h1, lateHazard: h2, ratio, mean, sd, cv,
    minGap: Math.min(...gaps), n: gaps.length, verdict };
}

const spikeLab = { symbols: {}, pending: 0, reported: false };

function requestSpikeHistory(code, end) {
  ws.send(JSON.stringify({
    ticks_history: code, style: 'ticks', count: 5000, end: end || 'latest',
  }));
}

function startSpikeLab(list) {
  for (const s of list) {
    const meta = SPIKE_META[s.label];
    if (!meta) continue;
    spikeLab.symbols[s.code] = {
      label: s.label, meta, quotes: [], batches: 0, oldest: null, done: false,
    };
    spikeLab.pending += 1;
    requestSpikeHistory(s.code);
  }
  if (spikeLab.pending) {
    console.log(`[SPIKE] collecting ${SPIKE_BATCHES} x 5000 ticks for ` +
      Object.values(spikeLab.symbols).map((s) => s.label).join(', '));
  }
}

// Deriv caps a single request at 5000 ticks, so history is walked backwards one
// batch at a time using the oldest timestamp seen so far.
function onSpikeHistory(code, prices, times) {
  const st = spikeLab.symbols[code];
  if (!st || st.done) return;
  st.quotes = prices.map(Number).concat(st.quotes);
  st.oldest = times[0];
  st.batches += 1;

  if (st.batches < SPIKE_BATCHES && prices.length >= 4999) {
    requestSpikeHistory(code, st.oldest - 1);
    return;
  }

  st.done = true;
  spikeLab.pending -= 1;
  const det = detectSpikes(st.quotes, st.meta.dir);
  const gaps = gapsBetween(det.indices);
  st.result = memorylessTest(gaps);
  st.spikes = det.indices.length;
  st.ticks = st.quotes.length;
  st.quotes = []; // analysis is done; do not hold tens of thousands of numbers
  console.log(`[SPIKE] ${st.label}: ${st.ticks} ticks, ${st.spikes} spikes, ` +
    (st.result.usable
      ? `meanGap ${st.result.mean.toFixed(0)} (nominal ${st.meta.nominal}), ` +
        `cv ${st.result.cv.toFixed(2)}, hazard ratio ${st.result.ratio.toFixed(2)} -> ${st.result.verdict}`
      : st.result.reason));
  reportSpikeLab();
}

function reportSpikeLab() {
  if (spikeLab.reported || spikeLab.pending > 0) return;
  const all = Object.values(spikeLab.symbols).filter((s) => s.done);
  if (!all.length) return;
  spikeLab.reported = true;

  const lines = all.map((s) => {
    if (!s.result.usable) return `${s.label}: ${s.result.reason}`;
    const r = s.result;
    const ratio = r.ratio === Infinity ? 'inf' : r.ratio.toFixed(2);
    return `${s.label}: gap ${r.mean.toFixed(0)} vs ${s.meta.nominal} nominal, ` +
      `spread ${r.cv.toFixed(2)}, waiting-pays ${ratio} - ${r.verdict}`;
  });

  const anyEdge = all.some((s) => s.result.usable && s.result.verdict !== 'NO EDGE');
  const conclusion = anyEdge
    ? 'At least one symbol shows a rising spike probability. Counting ticks is worth building a strategy around - ask for the entry rules next.'
    : 'Every symbol looks memoryless: the chance of a spike on the next tick does not rise as you wait. Counting ticks cannot produce an edge here, and any bot claiming otherwise is selling you a coin flip.';

  sendTelegram(
    `Spike analysis - Boom/Crash, ${all[0].ticks} ticks each\n\n` +
    `${lines.join('\n')}\n\n` +
    `"spread" is how tightly gaps cluster. 1.00 means purely random timing; ` +
    `below 0.75 means spikes arrive on a schedule you can anticipate.\n` +
    `"waiting-pays" is how much more likely a spike becomes once you have waited. ` +
    `1.00 means waiting does not help at all.\n\n${conclusion}`
  );
  console.log('[SPIKE] verdicts:', all.map((s) => `${s.label}=${s.result.verdict || 'n/a'}`).join(' '));
}

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
  // One position per symbol. Stacking a second call on V75 while the first is
  // still open doubles the risk on a single instrument and is the main reason
  // the old build felt noisy — the same trend kept re-announcing itself.
  const free = !st.open;

  if (free && (crossedUp || flipped) && cooledDown) {
    const { entry, sl, tp, risk, reward } = levelsFor(direction, price, atrVal);
    const d = st.decimals ?? 2;
    const arrow = direction === 'BUY' ? '\u{1F7E2}' : '\u{1F534}';
    const g = holdGuidance();

    sendTelegram(
      `${arrow} *${direction} ${label} NOW*\n\n` +
      `Entry  ${entry.toFixed(d)}\n` +
      `SL     ${sl.toFixed(d)}  (${direction === 'BUY' ? '-' : '+'}${risk.toFixed(d)})\n` +
      `TP     ${tp.toFixed(d)}  (${direction === 'BUY' ? '+' : '-'}${reward.toFixed(d)})\n` +
      `R:R    ${rrLabel}\n\n` +
      `Hold   about ${humanDuration(g.median)}, usually ${humanDuration(g.p25)} to ${humanDuration(g.p75)}\n` +
      `Close  after ${humanDuration(TIME_STOP_BARS)} if neither level is hit\n\n` +
      `*Confidence ${confidence}%*\n` +
      `RSI(14) ${rsiVal.toFixed(1)} · ATR(14) ${atrVal.toFixed(d)}`,
      { markdown: true }
    );

    st.open = {
      direction, entry, sl, tp, risk, confidence,
      bars: 0, openedAt: Date.now(), openBarTime: st.candles.at(-1).time,
    };
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

// ---- Open trade tracking ----
// Called on every price update. Watches the live candle's high and low rather
// than its close, because a stop is hit the moment price trades through it, not
// only when a candle happens to close beyond it.
function checkOpenTrade(code, label) {
  const st = state[code];
  const o = st?.open;
  if (!o) return;
  const bar = st.candles.at(-1);
  if (!bar) return;

  // Count elapsed candles by bar timestamp, so a reconnect or a burst of
  // updates inside one candle cannot inflate the hold time.
  if (bar.time !== o.lastBarTime) {
    o.lastBarTime = bar.time;
    if (bar.time !== o.openBarTime) o.bars += 1;
  }

  const long = o.direction === 'BUY';
  let outcome = null;
  let exit = null;

  // If a single candle straddles both levels there is no way to know which came
  // first, so assume the stop — the conservative reading, and the one that
  // avoids reporting a win that may not have happened.
  const hitSl = long ? bar.low <= o.sl : bar.high >= o.sl;
  const hitTp = long ? bar.high >= o.tp : bar.low <= o.tp;
  if (hitSl) { outcome = 'SL'; exit = o.sl; }
  else if (hitTp) { outcome = 'TP'; exit = o.tp; }
  else if (o.bars >= TIME_STOP_BARS) { outcome = 'TIME'; exit = bar.close; }
  if (!outcome) return;

  const d = st.decimals ?? 2;
  const rMultiple = ((long ? exit - o.entry : o.entry - exit) / o.risk);
  const rTxt = `${rMultiple >= 0 ? '+' : ''}${rMultiple.toFixed(1)}R`;
  const held = `${o.bars} candle${o.bars === 1 ? '' : 's'} (${humanDuration(o.bars)})`;

  if (outcome === 'TP') outcomes.tp += 1;
  else if (outcome === 'SL') outcomes.sl += 1;
  else outcomes.timeout += 1;
  // Timed-out trades never reached a level, so they would skew the hold stats.
  if (outcome !== 'TIME') outcomes.resolvedBars.push(o.bars);

  const header = outcome === 'TP' ? '✅ *TARGET HIT*'
    : outcome === 'SL' ? '❌ *STOP HIT*'
    : '⏰ *TIME STOP*';
  const closer = outcome === 'TIME'
    ? `\nNeither level hit in ${humanDuration(TIME_STOP_BARS)} — consider closing.`
    : '';

  sendTelegram(
    `${header} — ${label} ${o.direction}\n\n` +
    `Entry  ${o.entry.toFixed(d)}\n` +
    `Exit   ${exit.toFixed(d)}\n` +
    `Result ${rTxt} · held ${held}${closer}`,
    { markdown: true }
  );
  console.log(`[CLOSE] ${label} ${o.direction} ${outcome} ${rTxt} after ${o.bars} bars`);

  st.open = null;
  // Start the cooldown from the close, not the entry, so the next alert on this
  // symbol is genuinely a fresh setup rather than the tail of the last one.
  st.lastAlertAt = Date.now();
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
        lastAlertAt: 0, lastSignal: null, decimals: null, open: null,
      };
    }
    labels[s.code] = s.label;
    ws.send(JSON.stringify({
      ticks_history: s.code,
      style: 'candles',
      granularity: GRANULARITY,
      count: HISTORY_BARS,
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
      startSpikeLab(activeSymbols);
      return;
    }

    // Tick history comes back as msg_type 'history', separate from the candle
    // stream, and is used only for the Boom/Crash spike analysis.
    if (data.msg_type === 'history' && data.history && data.echo_req?.ticks_history) {
      onSpikeHistory(data.echo_req.ticks_history, data.history.prices, data.history.times);
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
      const full = data.candles.map((c) => ({
        time: c.epoch, open: +c.open, high: +c.high, low: +c.low, close: +c.close,
      }));
      // Deriv hands us thousands of real candles on the first response. Replaying
      // the strategy over them costs nothing and produces calibration figures
      // from this instrument's actual behaviour, which beats any assumption
      // about what a synthetic index does.
      backtestSymbol(reqSymbol, labels[reqSymbol] || reqSymbol, full);
      reportBacktest();
      st.candles = full.slice(-150);
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
      // Manage the existing position before looking for a new one, so a trade
      // that just closed frees the symbol on this same update.
      checkOpenTrade(symCode, labels[symCode] || symCode);
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
      risk: { slAtrMult: SL_ATR_MULT, tpAtrMult: TP_ATR_MULT, rr: rrLabel, timeStopBars: TIME_STOP_BARS },
      hold: holdGuidance(),
      backtest: {
        days: Math.round(backtest.days),
        signals: backtest.signals,
        perDay: +(backtest.signals / Math.max(backtest.days, 1)).toFixed(2),
        tp: backtest.tp, sl: backtest.sl, timeout: backtest.timeout,
      },
      liveOutcomes: { tp: outcomes.tp, sl: outcomes.sl, timeout: outcomes.timeout },
      spikeAnalysis: Object.fromEntries(
        Object.values(spikeLab.symbols).filter((s) => s.done).map((s) => [s.label, {
          ticks: s.ticks, spikes: s.spikes, nominalGap: s.meta.nominal,
          measuredGap: s.result.usable ? Math.round(s.result.mean) : null,
          gapSpread: s.result.usable ? +s.result.cv.toFixed(2) : null,
          waitingPays: s.result.usable
            ? (s.result.ratio === Infinity ? 'inf' : +s.result.ratio.toFixed(2)) : null,
          verdict: s.result.usable ? s.result.verdict : s.result.reason,
        }])
      ),
      openTrades: Object.fromEntries(
        Object.entries(state)
          .filter(([, s]) => s.open)
          .map(([code, s]) => [labels[code] || code, {
            direction: s.open.direction, entry: s.open.entry,
            sl: s.open.sl, tp: s.open.tp, barsHeld: s.open.bars,
          }])
      ),
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
