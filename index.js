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

// Boom and Crash generate far more signals than the volatility indices - in the
// last backtest B1000 fired 104 against V50's 35 - because their constant small
// drift keeps nudging the trend score. Holding them to a higher bar cuts the
// noise without silencing them, since their engineered spike timing is the only
// non-random structure in the whole product range.
// Override per symbol with e.g. SYMBOL_THRESHOLDS="B500:82,C1000:80".
const SPIKE_THRESHOLD_BUMP = Number(process.env.SPIKE_THRESHOLD_BUMP || 6);

const SYMBOL_THRESHOLDS = (() => {
  const map = {};
  for (const label of ['B500', 'B1000', 'C500', 'C1000']) {
    map[label] = CONFIDENCE_THRESHOLD + SPIKE_THRESHOLD_BUMP;
  }
  for (const part of (process.env.SYMBOL_THRESHOLDS || '').split(',')) {
    const [label, value] = part.split(':').map((s) => (s || '').trim());
    if (label && Number.isFinite(Number(value))) map[label] = Number(value);
  }
  return map;
})();

function thresholdFor(label) {
  return SYMBOL_THRESHOLDS[label] ?? CONFIDENCE_THRESHOLD;
}
const GRANULARITY = Number(process.env.GRANULARITY_SECONDS || 900); // 900 = M15
const ALERT_COOLDOWN_MS = Number(process.env.ALERT_COOLDOWN_MINUTES || 15) * 60 * 1000;

// ---- Selection mode ----
// The original design alerts the instant any symbol crosses its threshold. That
// is first-come-first-served, not best-available: a 74 on V10 at 09:00 takes the
// slot that an 88 on V75 would have filled at 09:20, and the position cap means
// the good setup is then turned away.
//
// In selection mode the bot instead wakes on a fixed schedule, scores all ten
// symbols at the same moment, ranks them, and takes only the strongest - and
// only if it clears a floor. If nothing clears, it sends nothing. Silence is a
// valid answer and the most common one at a high floor.
//
// Set DECISION_INTERVAL_MINUTES=0 to return to the original cross-triggered
// behaviour.
const DECISION_INTERVAL_MINUTES = Number(process.env.DECISION_INTERVAL_MINUTES ?? 240);
const DECISION_TOP_K = Number(process.env.DECISION_TOP_K || 1);
// Defaults to the ordinary threshold so turning selection mode on does not
// silently also change how selective the bot is - one variable, one effect.
const DECISION_FLOOR = Number(process.env.DECISION_FLOOR || CONFIDENCE_THRESHOLD);
const SELECTION_MODE = DECISION_INTERVAL_MINUTES > 0;

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

// ---- Risk management ----
// Position size is reported in risk-per-point rather than lots, because lot size
// depends on each instrument's contract specification and getting that wrong is
// worse than not stating it. Set ACCOUNT_BALANCE to your real balance and this
// becomes a concrete figure instead of a percentage.
const ACCOUNT_BALANCE = Number(process.env.ACCOUNT_BALANCE || 0);
const RISK_PERCENT = Number(process.env.RISK_PERCENT || 1);
const ACCOUNT_CURRENCY = (process.env.ACCOUNT_CURRENCY || 'USD').trim();

// Stops opening anything new once the day is far enough underwater. Counted in R
// so it is independent of position size. Resets at UTC midnight.
const DAILY_LOSS_LIMIT_R = Number(process.env.DAILY_LOSS_LIMIT_R || 3);
// Ten symbols each holding a position means ten times the risk at once.
const MAX_OPEN_TRADES = Number(process.env.MAX_OPEN_TRADES || 3);

// Move the stop to entry once the trade is this far in profit, then trail it.
// Both default to OFF because the backtest says they hurt: on a series with no
// drift, price reaching +1R is no more likely to continue than to reverse, so
// break-even converts eventual 2R winners into 0R scratches while doing nothing
// about losses that run straight to the stop. Measured expectancy went from
// -0.217R (fixed) to -0.220R (break-even) to -0.233R (break-even plus trail),
// with the win rate halving. The startup sweep re-measures this on your own
// data every restart - turn them on only if that sweep disagrees.
const BREAKEVEN_AT_R = Number(process.env.BREAKEVEN_AT_R ?? 0);
const TRAIL_AFTER_R = Number(process.env.TRAIL_AFTER_R ?? 0);
const TRAIL_DISTANCE_R = Number(process.env.TRAIL_DISTANCE_R || 1);

// ---- Trading costs ----
// Every result so far has assumed you get the exact quoted price. You do not:
// there is a spread on entry, and it is a fixed cost per trade. Expressed as a
// fraction of ATR so it scales across instruments the way everything else does.
//
// The consequence people miss: cost in R is SPREAD_ATR divided by the stop
// multiple. Halving the stop from 1.5 to 1.0 ATR made the same spread 50% more
// expensive per trade, so the setting that wins on gross numbers is not
// automatically the one that wins after costs. Applied inside the replay so the
// stop/target comparison ranks on net results, not gross.
const SPREAD_ATR = Number(process.env.SPREAD_ATR || 0);
const SPREAD_LEVELS = [0, 0.02, 0.05, 0.1, 0.2];

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
// Raised to the Deriv maximum so the entry scan can reach longer horizons: a
// 32-candle hold needs roughly 4000 candles before the non-overlapping sample is
// large enough to mean anything.
// Deriv silently caps candle history at 1000 per request no matter what you ask
// for - the same undocumented limit that applies to ticks. Asking for 5000 and
// receiving 1000 is why every backtest reported exactly 10.4 days and why the
// sample never grew past about 100 trades however often it re-ran.
// Walking backwards through history the way the spike analysis does turns that
// into roughly 200 days and 2000 trades, which is the difference between a
// suggestive result and a settled one.
const HISTORY_BARS = 1000;
const HISTORY_BATCHES = Number(process.env.HISTORY_BATCHES || 20);
const HISTORY_REQUEST_MS = Number(process.env.HISTORY_REQUEST_MS || 1500);
const HISTORY_MAX_RETRIES = Number(process.env.HISTORY_MAX_RETRIES || 6);

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
// Deriv caps tick history at 1000 per request regardless of what you ask for,
// and these instruments tick once a second, so one batch is about 17 minutes.
// 60 batches is roughly 17 hours per symbol, which yields well over 100 spikes
// on the 500-series and enough on the 1000-series to say something.
const SPIKE_BATCHES = Number(process.env.SPIKE_BATCHES || 80);
const SPIKE_MIN_GAPS = Number(process.env.SPIKE_MIN_GAPS || 80);
const SPIKE_BATCH_SIZE = Number(process.env.SPIKE_BATCH_SIZE || 1000);

// Deriv rate-limits ticks_history. Requests are drained from a single queue at
// this interval; the interval widens automatically if Deriv still pushes back.
const SPIKE_REQUEST_MS = Number(process.env.SPIKE_REQUEST_MS || 2000);
const SPIKE_MAX_RETRIES = Number(process.env.SPIKE_MAX_RETRIES || 6);
const SPIKE_MULT = Number(process.env.SPIKE_MULT || 8);

// Deriv returns roughly 78 instruments and only 35 are synthetic. The other 43
// are real markets - currencies and commodities - on the same feed and the same
// candle format. Scanning them costs one request each and answers the question
// that matters: is "no rule survives" a fact about this strategy, or a fact
// about instruments generated by a random number generator?
// These are analysed only. Nothing here is ever traded or alerted on.
const SCAN_OTHER_MARKETS = (process.env.SCAN_OTHER_MARKETS || 'true') !== 'false';
const SCAN_OTHER_LIMIT = Number(process.env.SCAN_OTHER_LIMIT || 12);
const SCAN_REQUEST_MS = Number(process.env.SCAN_REQUEST_MS || 1200);

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

// ---- Trade management ----
// Shared by the live monitor and the backtest so the two cannot drift apart.
// Returns where the stop should sit from the NEXT bar onward. Deliberately not
// applied to the current bar: if one candle both reaches the break-even trigger
// and touches the original stop, there is no way to know which came first, and
// assuming the favourable order would flatter every reported result.
function manageStop(open, bar, cfg) {
  const long = open.dir === 'BUY';
  const best = long ? bar.high : bar.low;
  const movedR = (long ? best - open.entry : open.entry - best) / open.risk;
  let sl = open.sl;

  if (cfg.beAtR > 0 && movedR >= cfg.beAtR) {
    sl = long ? Math.max(sl, open.entry) : Math.min(sl, open.entry);
  }
  if (cfg.trailAfterR > 0 && movedR >= cfg.trailAfterR) {
    const trail = long
      ? best - cfg.trailDistR * open.risk
      : best + cfg.trailDistR * open.risk;
    sl = long ? Math.max(sl, trail) : Math.min(sl, trail);
  }
  return sl;
}

const LIVE_MGMT = {
  beAtR: BREAKEVEN_AT_R, trailAfterR: TRAIL_AFTER_R, trailDistR: TRAIL_DISTANCE_R,
};

// ---- Daily risk budget ----
// Counted in R so it does not depend on position size, and reset on the UTC day
// boundary. Without this the bot happily keeps firing through a losing streak.
const daily = { day: null, r: 0, announced: false, entries: 0, tp: 0, sl: 0, timeout: 0 };
// Kept since the process started, so a single good or bad day cannot be mistaken
// for the trend. Resets on restart, which the summary says out loud.
const career = { days: 0, r: 0, tp: 0, sl: 0, timeout: 0, since: new Date(Date.now()).toISOString().slice(0, 10) };

function rollDay() {
  const key = new Date(Date.now()).toISOString().slice(0, 10);
  if (daily.day === key) return;
  const closed = daily.tp + daily.sl + daily.timeout;
  if (daily.day !== null && closed > 0) sendDailySummary(closed);
  daily.day = key;
  daily.r = 0; daily.announced = false;
  daily.entries = 0; daily.tp = 0; daily.sl = 0; daily.timeout = 0;
}

// The point of this is not encouragement. It is to make the running result
// visible as a number, so a losing run is obvious early rather than felt late.
function sendDailySummary(closed) {
  career.days += 1;
  career.r += daily.r;
  career.tp += daily.tp; career.sl += daily.sl; career.timeout += daily.timeout;
  const careerClosed = career.tp + career.sl + career.timeout;
  const winRate = closed ? (100 * daily.tp) / closed : 0;
  const avg = closed ? daily.r / closed : 0;

  sendTelegram(
    `Daily summary — ${daily.day}\n\n` +
    `Trades closed: ${closed}\n` +
    `Hit target: ${daily.tp} · Hit stop: ${daily.sl} · Timed out: ${daily.timeout}\n` +
    `Win rate: ${winRate.toFixed(0)}%\n` +
    `Net: ${daily.r >= 0 ? '+' : ''}${daily.r.toFixed(1)}R ` +
    `(${avg >= 0 ? '+' : ''}${avg.toFixed(2)}R per trade)\n\n` +
    `Since ${career.since}: ${careerClosed} trades over ${career.days} day` +
    `${career.days === 1 ? '' : 's'}, ` +
    `${career.r >= 0 ? '+' : ''}${career.r.toFixed(1)}R total ` +
    `(${careerClosed ? (career.r / careerClosed >= 0 ? '+' : '') + (career.r / careerClosed).toFixed(2) : '0.00'}R per trade)\n\n` +
    'Break-even needs better than +0.00R per trade. Counts restart when the bot restarts.'
  );
  console.log(`[DAILY] ${daily.day}: ${closed} closed, ${daily.r.toFixed(2)}R | ` +
    `career ${careerClosed} trades ${career.r.toFixed(2)}R`);
}

function recordDailyR(r) { rollDay(); daily.r += r; }

function dailyLimitHit() {
  rollDay();
  return DAILY_LOSS_LIMIT_R > 0 && daily.r <= -DAILY_LOSS_LIMIT_R;
}

// Reports risk per point rather than a lot size, because contract size differs
// per instrument and a confidently wrong lot figure is worse than none.
function positionSizeLine(stopDistance, decimals) {
  if (!(ACCOUNT_BALANCE > 0) || !(stopDistance > 0)) {
    return `Size   risk ${RISK_PERCENT}% of account (set ACCOUNT BALANCE for the figure)`;
  }
  const riskAmount = (ACCOUNT_BALANCE * RISK_PERCENT) / 100;
  const perPoint = riskAmount / stopDistance;
  return `Size   risk ${riskAmount.toFixed(2)} ${ACCOUNT_CURRENCY} ` +
    `(${RISK_PERCENT}%) over ${stopDistance.toFixed(decimals)} pts = ` +
    `${perPoint.toFixed(4)} per point`;
}

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

// sumR2 is the running sum of squared results, kept so the spread of outcomes
// can be recovered later. Without it there is no way to tell a real difference
// between two groups from a lucky one.
function blankVariant() {
  return { signals: 0, tp: 0, sl: 0, timeout: 0, sumR: 0, sumR2: 0, bars: [] };
}

// Does letting the stop move actually help? With a 27% win rate the intuition is
// that break-even should convert losers into scratches, but intuition is exactly
// what this project keeps getting wrong, so it gets measured alongside the rest.
const MGMT_VARIANTS = [
  { key: 'fixed stop', cfg: null },
  { key: 'break-even at 1R', cfg: { beAtR: 1, trailAfterR: 0, trailDistR: 0 } },
  { key: 'break-even + trail', cfg: { beAtR: 1, trailAfterR: 1.5, trailDistR: 1 } },
  { key: 'break-even at 0.5R', cfg: { beAtR: 0.5, trailAfterR: 0, trailDistR: 0 } },
];

// Every stop/target paired with every stop rule, so the out-of-sample check
// covers the same choices the in-sample tables offer.
const SPLIT_COMBOS = (() => {
  const out = [];
  for (const v of RISK_VARIANTS) {
    for (const m of MGMT_VARIANTS) {
      out.push({ key: `SL${v.sl} TP${v.tp} ${m.key}`, sl: v.sl, tp: v.tp, cfg: m.cfg });
    }
  }
  return out;
})();

// Does the confidence score mean anything? The bot has been publishing numbers
// between the threshold and 95 without anyone ever checking whether a high one
// outperforms a low one. If it does, raising the threshold is free improvement.
// If it does not, the entire scoring apparatus is decoration and should be
// replaced or removed rather than tuned.
// The time stop was set at 24 candles when the median trade took 5. With tighter
// levels the median is nearer 3, so six hours may now be holding dead trades ten
// times longer than a typical winner needs. Measured rather than guessed.
const TIME_STOP_VARIANTS = [6, 12, 24, 48];

// Does it pay to trade only when the market is speeding up? Direction is not
// predictable, but ATR-based targets need movement to reach them, so entering
// into expanding volatility is a separate hypothesis that has never been tested.
const VOL_BUCKETS = ['volatility rising', 'volatility falling'];

const CONFIDENCE_BUCKETS = [
  { key: '74-77', lo: 0, hi: 78 },
  { key: '78-81', lo: 78, hi: 82 },
  { key: '82-85', lo: 82, hi: 86 },
  { key: '86+', lo: 86, hi: 999 },
];

// The confidence buckets say a high score pays better than a low one, but that
// is an in-sample observation on buckets chosen after seeing the data. Raising
// the threshold is only justified if the ranking survives on history the choice
// was not made on, so every candidate threshold is replayed on both halves and
// picked on the first one alone.
//
// Two numbers matter and they pull in opposite directions: expectancy per trade
// rises with the threshold, while trades per day falls. Which one to optimise
// depends on whether the account is capacity-constrained. With MAX_OPEN_TRADES
// capping concurrent positions, most filtered-out signals were never tradeable
// anyway, so per-trade quality is the honest target - but R per day is reported
// alongside it so the trade-off is visible rather than assumed.
// These are BASE thresholds, meaning the value CONFIDENCE_THRESHOLD would be set
// to. Boom and Crash carry a bump on top, and the sweep preserves each symbol's
// own offset - otherwise "78" would quietly remove the Boom/Crash filter while
// tightening everything else, and the winning row would not correspond to any
// setting that could actually be applied.
const THRESHOLD_VARIANTS = (() => {
  const set = new Set([72, 74, 76, 78, 80, 82, 84]);
  set.add(CONFIDENCE_THRESHOLD); // always measure what is actually running
  return [...set].sort((a, b) => a - b);
})();

// Would a slower chart suit a small account better? Holding fewer, longer trades
// is not obviously better or worse - it trades signal count for signal quality,
// and the only way to know which wins is to measure both. M15 candles are
// aggregated into H1 and H4 here rather than pulled separately, so all three
// timeframes are scored on exactly the same price history.
const TIMEFRAME_VARIANTS = [
  { key: 'M15 (live)', group: 1 },
  { key: 'H1', group: 4 },
  { key: 'H4', group: 16 },
];

// Merges consecutive candles into one. Open comes from the first, close from the
// last, high and low from the extremes - the standard aggregation, and the same
// one MT5 would show if the chart period were changed.
function aggregateCandles(candles, group) {
  if (group <= 1) return candles;
  const out = [];
  for (let i = 0; i + group <= candles.length; i += group) {
    const slice = candles.slice(i, i + group);
    out.push({
      time: slice[0].time,
      open: slice[0].open,
      close: slice.at(-1).close,
      high: Math.max(...slice.map((c) => c.high)),
      low: Math.min(...slice.map((c) => c.low)),
    });
  }
  return out;
}

const backtest = {
  done: 0, days: 0, reported: false,
  byConfidence: Object.fromEntries(CONFIDENCE_BUCKETS.map((b) => [b.key, blankVariant()])),
  byVolatility: Object.fromEntries(VOL_BUCKETS.map((k) => [k, blankVariant()])),
  byTimeStop: Object.fromEntries(TIME_STOP_VARIANTS.map((t) => [`${t} candles`, blankVariant()])),
  byThreshold: Object.fromEntries(THRESHOLD_VARIANTS.map((t) => [String(t), blankVariant()])),
  byTimeframe: Object.fromEntries(TIMEFRAME_VARIANTS.map((t) => [t.key, blankVariant()])),
  thresholdSplit: {
    inSample: Object.fromEntries(THRESHOLD_VARIANTS.map((t) => [String(t), blankVariant()])),
    outSample: Object.fromEntries(THRESHOLD_VARIANTS.map((t) => [String(t), blankVariant()])),
  },
  split: {
    inSample: Object.fromEntries(SPLIT_COMBOS.map((c) => [c.key, blankVariant()])),
    outSample: Object.fromEntries(SPLIT_COMBOS.map((c) => [c.key, blankVariant()])),
  },
  mgmt: Object.fromEntries(MGMT_VARIANTS.map((m) => [m.key, blankVariant()])),
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
// Scoring is by far the expensive part and does not depend on stop placement,
// so it runs once per bar and every variant reuses the result. Pulled out of
// backtestSymbol so the same code scores the aggregated H1 and H4 series - if
// the timeframe study used its own copy the comparison would be meaningless.
function scoreSeries(candles) {
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
  return scores;
}

// One replay engine, driven by a risk setting and a management mode, so the
// stop/target sweep, the break-even sweep, the threshold sweep and the timeframe
// study cannot disagree with each other or with the live monitor.
// `studies` is false for anything that is not the live configuration, so the
// confidence and volatility tables only ever describe trades you would actually
// have received.
function makeReplay(candles, scores, cooldownBars, defaultThreshold, studies) {
  return (slMult, tpMult, mgmt, acc, from = 60, to = candles.length,
          timeStop = TIME_STOP_BARS, threshold = defaultThreshold) => {
    let lastConf = 0, lastDir = null, lastAlertBar = -1e9, open = null;

    for (let i = from; i < to; i++) {
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
        else if (held >= timeStop) out = 'TIME';

        if (out) {
          // Measure the realised result rather than assuming a full win or loss:
          // once the stop can move, "stop hit" no longer means exactly -1R.
          const exit = out === 'SL' ? open.sl : out === 'TP' ? open.tp : bar.close;
          // Risk is slMult x ATR and the spread is SPREAD_ATR x ATR, so the cost
          // in R is simply the ratio of the two - the ATR cancels.
          const costR = slMult > 0 ? SPREAD_ATR / slMult : 0;
          const r = (long ? exit - open.entry : open.entry - exit) / open.risk - costR;
          if (out === 'TP') acc.tp++;
          else if (out === 'SL') acc.sl++;
          else acc.timeout++;
          acc.sumR += r;
          // Squared results too, so the spread of outcomes can be recovered and
          // any two accumulators compared properly instead of by eye.
          acc.sumR2 += r * r;
          if (out !== 'TIME') acc.bars.push(held);
          // Only the configured settings feed the confidence study, so it
          // describes the trades you actually receive.
          if (studies && acc === backtest.active && open.confidence != null) {
            const bucket = CONFIDENCE_BUCKETS.find(
              (bk) => open.confidence >= bk.lo && open.confidence < bk.hi);
            const record = (store) => {
              store.signals++; store.sumR += r; store.sumR2 += r * r;
              if (out === 'TP') store.tp++; else if (out === 'SL') store.sl++; else store.timeout++;
            };
            if (bucket) record(backtest.byConfidence[bucket.key]);
            if (open.volRising != null) {
              record(backtest.byVolatility[open.volRising ? VOL_BUCKETS[0] : VOL_BUCKETS[1]]);
            }
          }
          open = null;
          lastAlertBar = i;
        } else if (mgmt) {
          open.sl = manageStop(open, bar, mgmt);
        }
        continue;
      }

      const sc = scores[i];
      if (!sc) continue;
      // Same per-symbol bar the live monitor uses, so the backtest keeps
      // reporting what you will actually receive.
      const strong = sc.confidence >= threshold;
      const crossedUp = strong && lastConf < threshold;
      const flipped = strong && lastDir !== null && lastDir !== sc.direction;
      if ((crossedUp || flipped) && (i - lastAlertBar) > cooldownBars) {
        const long = sc.direction === 'BUY';
        const risk = slMult * sc.atrVal;
        const reward = tpMult * sc.atrVal;
        // Was the market speeding up or slowing down as this trade opened?
        const past = scores[i - 5];
        open = {
          i, dir: sc.direction, entry: sc.price, risk, confidence: sc.confidence,
          volRising: past ? sc.atrVal > past.atrVal : null,
          sl: long ? sc.price - risk : sc.price + risk,
          tp: long ? sc.price + reward : sc.price - reward,
        };
        acc.signals++;
      }
      lastConf = sc.confidence;
      lastDir = sc.direction;
    }
  };
}

// Replays exactly the live gating rules over historical candles: same score,
// same threshold cross / flip trigger, same cooldown, same one-position rule,
// same time stop. If this disagrees with live behaviour later, the rules here
// have drifted from evaluateSignal and that is a bug, not a market change.
function backtestSymbol(code, label, candles) {
  if (candles.length < 200) return;
  const cooldownBars = Math.max(1, Math.round(ALERT_COOLDOWN_MS / (GRANULARITY * 1000)));
  const liveThreshold = thresholdFor(label);
  const scores = scoreSeries(candles);
  // Kept aside so the cross-symbol study can compare all ten at the same instant
  // once the last symbol has landed.
  keepForPortfolio(label, candles, scores);
  const replay = makeReplay(candles, scores, cooldownBars, liveThreshold, true);

  // Stop/target sweep, all with fixed stops so the comparison is clean.
  for (const v of RISK_VARIANTS) replay(v.sl, v.tp, null, backtest.variants[variantKey(v)]);
  // Management sweep, held at the configured stop/target so the only thing
  // changing is whether the stop is allowed to move.
  for (const m of MGMT_VARIANTS) replay(SL_ATR_MULT, TP_ATR_MULT, m.cfg, backtest.mgmt[m.key]);
  // Time-stop sweep at the configured levels, so the only thing changing is how
  // long a trade is allowed to go nowhere.
  for (const t of TIME_STOP_VARIANTS) {
    replay(SL_ATR_MULT, TP_ATR_MULT, LIVE_MGMT.beAtR > 0 ? LIVE_MGMT : null,
      backtest.byTimeStop[`${t} candles`], 60, candles.length, t);
  }

  // Out-of-sample check. Picking the best of a dozen settings on one stretch of
  // history will always produce a flattering number - some of that result is the
  // setting fitting the noise. So every combination is also run separately on
  // the first and second halves. The winner is chosen on the first half only,
  // and judged on the second, which it never saw.
  const mid = Math.floor((60 + candles.length) / 2);
  for (const combo of SPLIT_COMBOS) {
    replay(combo.sl, combo.tp, combo.cfg, backtest.split.inSample[combo.key], 60, mid);
    replay(combo.sl, combo.tp, combo.cfg, backtest.split.outSample[combo.key], mid, candles.length);
  }

  // Threshold sweep. Run on the full history for the headline table, and on each
  // half separately so the choice can be made on data the verdict is not read
  // from. Everything else is held at the live settings so the only thing moving
  // is how selective the bot is.
  const liveCfg = LIVE_MGMT.beAtR > 0 || LIVE_MGMT.trailAfterR > 0 ? LIVE_MGMT : null;
  // Whatever extra selectivity this symbol already carries travels with it, so
  // each row is a base threshold that could be set in Render as-is.
  const symbolOffset = liveThreshold - CONFIDENCE_THRESHOLD;
  for (const t of THRESHOLD_VARIANTS) {
    const k = String(t);
    const applied = t + symbolOffset;
    replay(SL_ATR_MULT, TP_ATR_MULT, liveCfg, backtest.byThreshold[k],
      60, candles.length, TIME_STOP_BARS, applied);
    replay(SL_ATR_MULT, TP_ATR_MULT, liveCfg, backtest.thresholdSplit.inSample[k],
      60, mid, TIME_STOP_BARS, applied);
    replay(SL_ATR_MULT, TP_ATR_MULT, liveCfg, backtest.thresholdSplit.outSample[k],
      mid, candles.length, TIME_STOP_BARS, applied);
  }

  const spanDays = (candles.at(-1).time - candles[0].time) / 86400;

  // Timeframe study. The same history, merged into bigger candles, scored by the
  // same function and replayed by the same engine.
  //
  // The time stop stays at the same number of BARS rather than the same number
  // of hours. Holding it at six hours across every timeframe was the obvious
  // thing to do and it is wrong: stops and targets are ATR multiples, and ATR on
  // H4 is roughly four times the M15 figure, so an H4 trade has sixteen times
  // further to travel. Capping it at six hours would time nearly all of them out
  // and report the slower charts as hopeless when what had actually been
  // measured was the time stop. In bars, each timeframe is judged at its own
  // natural pace - which is also the whole point of asking the question.
  for (const tf of TIMEFRAME_VARIANTS) {
    const merged = tf.group === 1 ? candles : aggregateCandles(candles, tf.group);
    if (merged.length < 200) continue;
    const tfScores = tf.group === 1 ? scores : scoreSeries(merged);
    const tfCooldown = Math.max(1, Math.round(cooldownBars / tf.group));
    const tfTimeStop = TIME_STOP_BARS;
    const tfReplay = makeReplay(merged, tfScores, tfCooldown, liveThreshold, false);
    const acc = backtest.byTimeframe[tf.key];
    tfReplay(SL_ATR_MULT, TP_ATR_MULT, liveCfg, acc, 60, merged.length, tfTimeStop);
    // Hold time is in bars, and a bar means something different on each
    // timeframe, so record the span in days to make the study comparable.
    acc.spanDays = (acc.spanDays || 0) + spanDays;
    acc.barMinutes = (GRANULARITY / 60) * tf.group;
  }

  backtest.done += 1;
  backtest.days = Math.max(backtest.days, spanDays);
  const a = backtest.active;
  console.log(
    `[BACKTEST] ${label}: ${spanDays.toFixed(1)}d, active ${ACTIVE_VARIANT} -> ` +
    `${a.signals} signals TP=${a.tp} SL=${a.sl} timeout=${a.timeout}`
  );
}

// ---- Selection-mode study ----
// Everything above measures one symbol at a time, which cannot answer the
// question selection mode raises: if the bot waited, looked at all ten together
// and took only the strongest, would it do better?
//
// Answering it needs the symbols aligned in time, so each one's scored series is
// kept as typed arrays until the last symbol lands. Ten symbols at 20,000 bars
// costs roughly 9MB this way; keeping the candle objects would cost forty times
// that and the free tier would not survive it.
const portfolioLab = { symbols: [], ran: false };

function keepForPortfolio(label, candles, scores) {
  const n = candles.length;
  const time = new Float64Array(n);
  const high = new Float64Array(n);
  const low = new Float64Array(n);
  const close = new Float64Array(n);
  const atrArr = new Float64Array(n);
  const conf = new Int16Array(n);
  const dir = new Int8Array(n); // +1 buy, -1 sell, 0 no score

  for (let i = 0; i < n; i++) {
    time[i] = candles[i].time;
    high[i] = candles[i].high;
    low[i] = candles[i].low;
    close[i] = candles[i].close;
    const s = scores[i];
    if (s) {
      conf[i] = s.confidence;
      dir[i] = s.direction === 'BUY' ? 1 : -1;
      atrArr[i] = s.atrVal;
    }
  }
  portfolioLab.symbols.push({ label, n, time, high, low, close, atr: atrArr, conf, dir });
}

// Candidate schedules. Expressed in bars so they follow GRANULARITY rather than
// assuming M15.
const DECISION_CADENCES = [
  { key: 'every bar', bars: 1 },
  { key: 'hourly', bars: Math.max(1, Math.round(3600 / GRANULARITY)) },
  { key: '4-hourly', bars: Math.max(1, Math.round(14400 / GRANULARITY)) },
  { key: 'daily', bars: Math.max(1, Math.round(86400 / GRANULARITY)) },
];
const DECISION_TOPKS = [1, 2, 3];
const DECISION_FLOORS = [74, 78, 82];

// One portfolio replay: walk the shared clock, manage whatever is open, and at
// each scheduled moment take the strongest qualifying symbols up to the cap.
function portfolioReplay({ cadenceBars, topK, floor, from = 0, to = 1 }) {
  const syms = portfolioLab.symbols;
  if (!syms.length) return null;

  let start = Infinity, end = -Infinity;
  for (const s of syms) {
    if (s.n) { start = Math.min(start, s.time[0]); end = Math.max(end, s.time[s.n - 1]); }
  }
  if (!Number.isFinite(start)) return null;

  const step = GRANULARITY;
  const total = Math.floor((end - start) / step) + 1;
  const gFrom = Math.floor(total * from);
  const gTo = Math.floor(total * to);

  // time -> bar index, per symbol. Built once and reused across the grid.
  const index = syms.map((s) => {
    const m = new Map();
    for (let i = 0; i < s.n; i++) m.set(s.time[i], i);
    return m;
  });

  const open = new Array(syms.length).fill(null);
  const cfg = LIVE_MGMT.beAtR > 0 || LIVE_MGMT.trailAfterR > 0 ? LIVE_MGMT : null;
  const costR = SL_ATR_MULT > 0 ? SPREAD_ATR / SL_ATR_MULT : 0;

  let tp = 0, sl = 0, timeout = 0, sumR = 0, sumR2 = 0, rounds = 0, emptyRounds = 0;
  const holds = [];

  for (let g = gFrom; g < gTo; g++) {
    const t = start + g * step;

    // --- manage open positions ---
    for (let k = 0; k < syms.length; k++) {
      const o = open[k];
      if (!o) continue;
      const s = syms[k];
      const i = index[k].get(t);
      if (i === undefined) continue;
      const long = o.dir === 1;
      const held = g - o.g;
      const hitSl = long ? s.low[i] <= o.sl : s.high[i] >= o.sl;
      const hitTp = long ? s.high[i] >= o.tp : s.low[i] <= o.tp;
      let out = null;
      // Stop first: a candle spanning both levels gives no way to know which
      // traded first, and assuming the loss is the honest default.
      if (hitSl) out = 'SL';
      else if (hitTp) out = 'TP';
      else if (held >= TIME_STOP_BARS) out = 'TIME';

      if (out) {
        const exit = out === 'SL' ? o.sl : out === 'TP' ? o.tp : s.close[i];
        const r = (long ? exit - o.entry : o.entry - exit) / o.risk - costR;
        if (out === 'TP') tp++; else if (out === 'SL') sl++; else timeout++;
        sumR += r; sumR2 += r * r;
        if (out !== 'TIME') holds.push(held);
        open[k] = null;
      } else if (cfg) {
        o.sl = manageStop({ dir: long ? 'BUY' : 'SELL', entry: o.entry, risk: o.risk, sl: o.sl },
          { high: s.high[i], low: s.low[i] }, cfg);
      }
    }

    // --- scheduled decision ---
    if ((g - gFrom) % cadenceBars !== 0) continue;
    rounds++;

    const openCount = open.filter(Boolean).length;
    const room = Math.min(MAX_OPEN_TRADES - openCount, topK);
    if (room <= 0) { emptyRounds++; continue; }

    const candidates = [];
    for (let k = 0; k < syms.length; k++) {
      if (open[k]) continue;
      const s = syms[k];
      const i = index[k].get(t);
      if (i === undefined || !s.dir[i] || !s.atr[i]) continue;
      // Each symbol keeps whatever extra selectivity it already carries.
      const symFloor = Math.max(floor, thresholdFor(s.label));
      if (s.conf[i] < symFloor) continue;
      candidates.push({ k, i, conf: s.conf[i] });
    }
    if (!candidates.length) { emptyRounds++; continue; }

    candidates.sort((a, b) => b.conf - a.conf);
    for (const c of candidates.slice(0, room)) {
      const s = syms[c.k];
      const long = s.dir[c.i] === 1;
      const risk = SL_ATR_MULT * s.atr[c.i];
      const reward = TP_ATR_MULT * s.atr[c.i];
      const entry = s.close[c.i];
      open[c.k] = {
        g, dir: s.dir[c.i], entry, risk,
        sl: long ? entry - risk : entry + risk,
        tp: long ? entry + reward : entry - reward,
      };
    }
  }

  const n = tp + sl + timeout;
  if (!n) return null;
  const days = ((gTo - gFrom) * step) / 86400;
  const exp = sumR / n;
  const varR = Math.max(sumR2 / n - exp * exp, 0);
  return {
    trades: n, tp, sl, timeout,
    winPct: (100 * tp) / n,
    expectancyR: exp,
    se: n > 1 ? Math.sqrt(varR / n) : null,
    t: n > 1 && varR > 0 ? exp / Math.sqrt(varR / n) : null,
    tradesPerDay: n / Math.max(days, 1),
    rPerDay: sumR / Math.max(days, 1),
    rounds, emptyRounds,
    quietPct: rounds ? (100 * emptyRounds) / rounds : 0,
    medianHoldBars: holds.length ? holds.sort((a, b) => a - b)[Math.floor(holds.length / 2)] : null,
  };
}

// The full comparison. Every schedule against every selectivity, judged on R per
// day - because a setting that earns more per trade while trading a tenth as
// often is a worse business, not a better one.
//
// 36 combinations is enough that the best will look good by luck, so the verdict
// comes from the out-of-sample split rather than the winning row.
const selectionStudy = { done: false, grid: [], chosen: null, oos: null, baseline: null, live: null };

function runSelectionStudy() {
  if (portfolioLab.ran || portfolioLab.symbols.length < 2) return;
  portfolioLab.ran = true;

  const combos = [];
  for (const c of DECISION_CADENCES) {
    for (const k of DECISION_TOPKS) {
      for (const f of DECISION_FLOORS) combos.push({ cadence: c, topK: k, floor: f });
    }
  }

  for (const combo of combos) {
    const full = portfolioReplay({
      cadenceBars: combo.cadence.bars, topK: combo.topK, floor: combo.floor,
    });
    if (!full) continue;
    selectionStudy.grid.push({
      key: `${combo.cadence.key}, top ${combo.topK}, floor ${combo.floor}`,
      cadence: combo.cadence.key, cadenceBars: combo.cadence.bars,
      topK: combo.topK, floor: combo.floor,
      ...full,
    });
  }
  if (!selectionStudy.grid.length) return;

  // Closest portfolio equivalent of the original behaviour: react every bar,
  // fill all three slots, no extra selectivity. Not identical to the live rule -
  // that one is first-come rather than best-available - but it is the fairest
  // like-for-like available from the same replay engine.
  selectionStudy.baseline = selectionStudy.grid.find(
    (g) => g.cadenceBars === 1 && g.topK === 3 && g.floor === 74) || null;

  // What is actually configured to run.
  selectionStudy.live = selectionStudy.grid.find(
    (g) => g.cadenceBars === Math.round((DECISION_INTERVAL_MINUTES * 60) / GRANULARITY) &&
           g.topK === DECISION_TOP_K && g.floor === DECISION_FLOOR) || null;

  // Choose on the first half, judge on the second.
  const first = [];
  for (const combo of combos) {
    const s = portfolioReplay({
      cadenceBars: combo.cadence.bars, topK: combo.topK, floor: combo.floor, from: 0, to: 0.5,
    });
    if (s && s.trades >= 30) {
      first.push({ combo, key: `${combo.cadence.key}, top ${combo.topK}, floor ${combo.floor}`, s });
    }
  }
  if (first.length) {
    const pick = first.reduce((a, b) => (b.s.rPerDay > a.s.rPerDay ? b : a));
    const after = portfolioReplay({
      cadenceBars: pick.combo.cadence.bars, topK: pick.combo.topK, floor: pick.combo.floor,
      from: 0.5, to: 1,
    });
    const baseAfter = portfolioReplay({ cadenceBars: 1, topK: 3, floor: 74, from: 0.5, to: 1 });
    // "Higher R per day" on its own is a coin flip: on pure noise the chosen
    // schedule beat the baseline in a quarter of trial runs simply because one
    // of two numbers has to be larger. So the verdict requires the gap to clear
    // the noise in both.
    //
    // The standard error of R per day follows from the per-trade one: total R
    // over the window has error se x n, and dividing by days gives
    // se x tradesPerDay. The two schedules share many of the same trades, which
    // makes the real error of the difference smaller than treating them as
    // independent - so this test is conservative, which is the right direction.
    let verdict = null;
    if (after && baseAfter && after.se != null && baseAfter.se != null) {
      const seA = after.se * after.tradesPerDay;
      const seB = baseAfter.se * baseAfter.tradesPerDay;
      const diff = after.rPerDay - baseAfter.rPerDay;
      const se = Math.sqrt(seA * seA + seB * seB);
      const t = se > 0 ? diff / se : null;
      verdict = {
        diffRPerDay: diff,
        t,
        beats: t != null && t > 2,
        label: t == null ? 'not measurable'
          : t > 2 ? 'BETTER'
          : t < -2 ? 'WORSE'
          : 'NO DIFFERENCE THAT CAN BE MEASURED',
      };
    }

    selectionStudy.chosen = pick.key;
    selectionStudy.oos = {
      chosenOnFirstHalf: pick.key,
      firstHalf: pick.s,
      secondHalf: after,
      baselineSecondHalf: baseAfter,
      verdict,
      beatsBaseline: verdict ? verdict.beats : null,
    };
  }

  selectionStudy.done = true;
  // The arrays are large and the study only runs once.
  portfolioLab.symbols.length = 0;
}

// Reported once, after every symbol has been replayed, so the user gets a single
// honest summary of what these settings would have produced recently.
// The honest test: choose the setting using only the first half of the history,
// then judge it on the second half it never saw. Exposed here rather than buried
// in the Telegram message so it can be read from the health page at any time.
function outOfSampleResult() {
  const expOf = (acc) => {
    const n = acc.tp + acc.sl + acc.timeout;
    return n ? { n, exp: acc.sumR / n, win: (100 * acc.tp) / n } : null;
  };
  const inS = SPLIT_COMBOS
    .map((c) => ({ key: c.key, s: expOf(backtest.split.inSample[c.key]) }))
    .filter((x) => x.s && x.s.n >= 15);
  if (!inS.length) return null;
  const pick = inS.reduce((a, b) => (b.s.exp > a.s.exp ? b : a));
  const after = expOf(backtest.split.outSample[pick.key]);
  return { pick, after, heldUp: !!(after && after.exp > 0) };
}

// Same discipline applied to the threshold. The confidence table already says a
// high score pays better, but that table was read off the same data that would
// justify the change, and a bucket boundary picked after the fact is exactly how
// a nonexistent edge gets adopted. So the threshold is chosen on the first half
// of history and reported on the second.
//
// Two guards keep this from recommending a threshold nobody could trade:
// a minimum trade count, and a report of what the choice costs in signals.
function thresholdOutOfSample() {
  const stat = (acc) => {
    const n = acc.tp + acc.sl + acc.timeout;
    if (!n) return null;
    const exp = acc.sumR / n;
    // Standard error of the mean R, so a difference can be judged rather than
    // eyeballed. sumR2 is carried for exactly this.
    const varR = Math.max(acc.sumR2 / n - exp * exp, 0);
    return {
      n, exp, win: (100 * acc.tp) / n,
      se: n > 1 ? Math.sqrt(varR / n) : null,
      t: n > 1 && varR > 0 ? exp / Math.sqrt(varR / n) : null,
    };
  };

  const table = THRESHOLD_VARIANTS.map((t) => {
    const full = stat(backtest.byThreshold[String(t)]);
    return full ? {
      threshold: t, ...full,
      perDay: backtest.byThreshold[String(t)].signals / Math.max(backtest.days, 1),
    } : null;
  }).filter(Boolean);

  const first = THRESHOLD_VARIANTS
    .map((t) => ({ threshold: t, s: stat(backtest.thresholdSplit.inSample[String(t)]) }))
    .filter((x) => x.s && x.s.n >= 100);
  if (!first.length) return { table, pick: null };

  const pick = first.reduce((a, b) => (b.s.exp > a.s.exp ? b : a));
  const after = stat(backtest.thresholdSplit.outSample[String(pick.threshold)]);
  const live = stat(backtest.thresholdSplit.outSample[String(CONFIDENCE_THRESHOLD)]);

  // The question is not "is the chosen threshold profitable" - almost all of
  // them are. It is "did choosing it beat leaving the setting alone", measured
  // on data neither choice was made on.
  let beatsLive = null;
  if (after && live && after.se != null && live.se != null && pick.threshold !== CONFIDENCE_THRESHOLD) {
    const diff = after.exp - live.exp;
    const se = Math.sqrt(after.se ** 2 + live.se ** 2);
    beatsLive = { diff, t: se > 0 ? diff / se : null };
  }

  return { table, pick, after, live, beatsLive };
}

// Does a slower chart suit a small account better? Compared on R per day rather
// than R per trade, because a setting that returns twice as much per trade while
// producing a tenth as many trades is a worse business, not a better one.
function timeframeStudy() {
  return TIMEFRAME_VARIANTS.map((tf) => {
    const a = backtest.byTimeframe[tf.key];
    const n = a.tp + a.sl + a.timeout;
    if (!n || !a.spanDays) return null;
    const days = a.spanDays / Math.max(activeSymbols.length, 1);
    const exp = a.sumR / n;
    const varR = Math.max(a.sumR2 / n - exp * exp, 0);
    const meanBars = a.bars.length ? a.bars.reduce((x, y) => x + y, 0) / a.bars.length : null;
    return {
      timeframe: tf.key,
      barMinutes: a.barMinutes,
      trades: n,
      winPct: +((100 * a.tp) / n).toFixed(1),
      expectancyR: +exp.toFixed(3),
      t: n > 1 && varR > 0 ? +(exp / Math.sqrt(varR / n)).toFixed(2) : null,
      tradesPerDay: +(n / Math.max(days, 1)).toFixed(2),
      rPerDay: +(a.sumR / Math.max(days, 1)).toFixed(3),
      typicalHoldHours: meanBars != null ? +((meanBars * a.barMinutes) / 60).toFixed(1) : null,
    };
  }).filter(Boolean);
}

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
  // Only possible now that every symbol has been scored and aligned.
  try {
    runSelectionStudy();
  } catch (err) {
    console.error('[SELECTION] study failed:', err.message);
  }

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

  const mgmtSummaries = MGMT_VARIANTS.map((m) => {
    const a = backtest.mgmt[m.key];
    const closed = a.tp + a.sl + a.timeout;
    return closed ? { key: m.key, closed, expectancy: a.sumR / closed, winRate: (100 * a.tp) / closed } : null;
  }).filter(Boolean);

  const mgmtRows = mgmtSummaries.length
    ? 'Stop management comparison (same levels, only the stop rule changes):\n' +
      mgmtSummaries
        .sort((a, b) => b.expectancy - a.expectancy)
        .map((s) => `${s.key} — win ${s.winRate.toFixed(0)}%, ` +
          `${s.expectancy >= 0 ? '+' : ''}${s.expectancy.toFixed(2)}R/trade`)
        .join('\n')
    : '';

  // Confidence study: does a higher score actually buy you a better trade?
  // A bucket needs enough trades to say anything, and a difference between two
  // buckets has to be larger than the noise in both of them combined. Without
  // that second check this reported "higher scores work" on data generated with
  // no edge at all, from a 37-trade bucket.
  const conf = CONFIDENCE_BUCKETS.map((bk) => {
    const a = backtest.byConfidence[bk.key];
    const n = a.tp + a.sl + a.timeout;
    if (n < 40) return null;
    const exp = a.sumR / n;
    const variance = Math.max(0, a.sumR2 / n - exp * exp);
    return { key: bk.key, n, exp, win: (100 * a.tp) / n, se: Math.sqrt(variance / n) };
  }).filter(Boolean);

  let confBlock = '';
  if (conf.length >= 2) {
    const lowest = conf[0], highest = conf[conf.length - 1];
    const diff = highest.exp - lowest.exp;
    const seDiff = Math.sqrt(lowest.se ** 2 + highest.se ** 2);
    const t = seDiff > 0 ? diff / seDiff : 0;
    const predictive = t > 2;
    confBlock = '\n\nDoes the confidence score predict anything?\n' +
      conf.map((c) => `${c.key}: ${c.n} trades, win ${c.win.toFixed(0)}%, ` +
        `${c.exp >= 0 ? '+' : ''}${c.exp.toFixed(2)}R`).join('\n') +
      `\nTop versus bottom: ${diff >= 0 ? '+' : ''}${diff.toFixed(2)}R difference, ` +
      `t ${t.toFixed(1)} (2.0 needed)\n` +
      (predictive
        ? 'Higher scores do produce better trades. Raising the threshold should help.'
        : 'The difference is within the noise. The score separates a signal from no ' +
          'signal, but beyond that the number carries no information - so raising the ' +
          'threshold only reduces how many trades you get, without improving them.');
  }

  // Same discipline for the two new questions: report the numbers, and only
  // claim a difference when it clears the noise in both groups combined.
  const summarise = (store, key) => {
    const a = store[key];
    const n = a.tp + a.sl + a.timeout;
    if (n < 40) return null;
    const exp = a.sumR / n;
    const variance = Math.max(0, a.sumR2 / n - exp * exp);
    return { key, n, exp, win: (100 * a.tp) / n, se: Math.sqrt(variance / n) };
  };
  const compare = (a, b) => {
    const se = Math.sqrt(a.se ** 2 + b.se ** 2);
    return se > 0 ? (b.exp - a.exp) / se : 0;
  };

  const vol = VOL_BUCKETS.map((k) => summarise(backtest.byVolatility, k)).filter(Boolean);
  let volBlock = '';
  if (vol.length === 2) {
    const t = compare(vol[1], vol[0]);
    volBlock = '\n\nDoes it pay to trade only when volatility is rising?\n' +
      vol.map((v) => `${v.key}: ${v.n} trades, win ${v.win.toFixed(0)}%, ` +
        `${v.exp >= 0 ? '+' : ''}${v.exp.toFixed(2)}R`).join('\n') +
      `\nDifference t ${t.toFixed(1)} (2.0 needed) - ` +
      (Math.abs(t) > 2
        ? (t > 0 ? 'rising volatility is genuinely better, worth filtering on.'
                 : 'falling volatility is genuinely better, which is the opposite of the usual advice.')
        : 'within the noise, so filtering on it would not help.');
  }

  const ts = TIME_STOP_VARIANTS.map((t) => summarise(backtest.byTimeStop, `${t} candles`)).filter(Boolean);
  let tsBlock = '';
  if (ts.length >= 2) {
    const bestTs = ts.reduce((a, b) => (b.exp > a.exp ? b : a));
    const current = ts.find((x) => x.key === `${TIME_STOP_BARS} candles`);
    tsBlock = '\n\nHow long should a stale trade be given?\n' +
      ts.map((x) => `${x.key} (${humanDuration(parseInt(x.key, 10))}): ${x.n} trades, ` +
        `${x.exp >= 0 ? '+' : ''}${x.exp.toFixed(2)}R` +
        (x.key === `${TIME_STOP_BARS} candles` ? ' (current)' : '')).join('\n') +
      (current && bestTs.key !== current.key && compare(current, bestTs) > 2
        ? `\nBest is ${bestTs.key}. Set TIME STOP BARS=${parseInt(bestTs.key, 10)} to switch.`
        : '\nNo option is clearly better than the current one.');
  }

  // Cost sensitivity. The per-trade cost is a constant, so the whole curve can
  // be read off the gross figure without replaying anything.
  const costPerR = 1 / SL_ATR_MULT;
  const gross = active.expectancy + SPREAD_ATR * costPerR;
  const costBlock = '\n\nWhat the spread does to this\n' +
    SPREAD_LEVELS.map((f) => {
      const net = gross - f * costPerR;
      return `spread ${f === 0 ? 'ignored' : `${f} x ATR`}: ` +
        `${net >= 0 ? '+' : ''}${net.toFixed(2)}R` +
        (Math.abs(f - SPREAD_ATR) < 1e-9 ? '  (assumed)' : '');
    }).join('\n') +
    `\nYour stop is ${SL_ATR_MULT}x ATR, so every 0.01 ATR of spread costs ` +
    `${(0.01 * costPerR).toFixed(3)}R per trade. Check the spread on your platform ` +
    `and read off the matching row - that is your real expectancy.`;

  const advice = best.key === ACTIVE_VARIANT
    ? 'Your current levels came out best of those tested.'
    : `Better on this data: SL ${best.key.split('/')[0]}x TP ${best.key.split('/')[1]}x ` +
      `(${best.expectancy >= 0 ? '+' : ''}${best.expectancy.toFixed(2)}R vs ` +
      `${active.expectancy >= 0 ? '+' : ''}${active.expectancy.toFixed(2)}R). ` +
      `Set SL ATR MULT=${best.key.split('/')[0]} and TP ATR MULT=${best.key.split('/')[1]} in Render to switch.`;

  const oos = outOfSampleResult();
  let oosBlock = '';
  if (oos) {
    const { pick, after, heldUp } = oos;
    oosBlock = '\n\nOut-of-sample check (chosen on the first half, judged on the second):\n' +
      `Best on first half: ${pick.key} — ${pick.s.exp >= 0 ? '+' : ''}${pick.s.exp.toFixed(2)}R ` +
      `over ${pick.s.n} trades\n` +
      (after
        ? `Same setting, second half: ${after.exp >= 0 ? '+' : ''}${after.exp.toFixed(2)}R ` +
          `over ${after.n} trades\n` +
          (heldUp
            ? 'It held up on data it was not chosen on. That is the only result here worth acting on.'
            : 'It did not hold up. The first-half number was the setting fitting noise, ' +
              'which is what picking the best of many always risks.')
        : 'Not enough second-half trades to judge.');
  }

  // The threshold is the one setting that trades quantity for quality directly,
  // so it gets the full treatment: full-history table, a choice made on the first
  // half only, and a test of whether that choice beats leaving it alone.
  const th = thresholdOutOfSample();
  let thBlock = '';
  if (th.table.length) {
    thBlock = '\n\nHow selective should the bot be?\n' +
      th.table.map((x) => `${x.threshold}%: ${x.perDay.toFixed(1)}/day, ${x.n} trades, ` +
        `win ${x.win.toFixed(0)}%, ${x.exp >= 0 ? '+' : ''}${x.exp.toFixed(2)}R` +
        (x.threshold === CONFIDENCE_THRESHOLD ? '  (current)' : '')).join('\n');
    if (th.pick && th.after) {
      thBlock += `\nChosen on the first half: ${th.pick.threshold}%. ` +
        `On the second half it never saw: ${th.after.exp >= 0 ? '+' : ''}${th.after.exp.toFixed(2)}R ` +
        `over ${th.after.n} trades.`;
      if (th.beatsLive) {
        thBlock += `\nAgainst the current ${CONFIDENCE_THRESHOLD}% on that same second half: ` +
          `${th.beatsLive.diff >= 0 ? '+' : ''}${th.beatsLive.diff.toFixed(2)}R difference, ` +
          `t ${th.beatsLive.t == null ? 'n/a' : th.beatsLive.t.toFixed(1)} (2.0 needed).\n` +
          (th.beatsLive.t != null && th.beatsLive.t > 2
            ? `Worth changing. Set CONFIDENCE THRESHOLD=${th.pick.threshold} in Render.`
            : 'Not proven. Leave the threshold where it is - fewer trades for an ' +
              'unproven gain is a worse position, not a safer one.');
      } else {
        thBlock += '\nThe first half picked the setting already running, so there is ' +
          'nothing to change.';
      }
    }
  }

  // Whether a slower chart suits the account better is a separate question from
  // how selective to be, and answering it on R per day keeps a low-frequency
  // setting from looking good purely because it trades rarely.
  const tfs = timeframeStudy();
  let tfBlock = '';
  if (tfs.length >= 2) {
    const bestTf = tfs.reduce((a, b) => (b.rPerDay > a.rPerDay ? b : a));
    tfBlock = '\n\nWould a slower chart suit the account better?\n' +
      tfs.map((x) => `${x.timeframe}: ${x.tradesPerDay.toFixed(1)} trades/day, ` +
        `win ${x.winPct.toFixed(0)}%, ${x.expectancyR >= 0 ? '+' : ''}${x.expectancyR.toFixed(2)}R each, ` +
        `${x.rPerDay >= 0 ? '+' : ''}${x.rPerDay.toFixed(2)}R/day` +
        (x.typicalHoldHours != null ? `, held ~${x.typicalHoldHours}h` : '')).join('\n') +
      `\nMost R per day: ${bestTf.timeframe}. ` +
      (bestTf.timeframe === 'M15 (live)'
        ? 'The current timeframe still earns the most per day, even though it trades more often.'
        : `A slower chart produced more per day here. Set GRANULARITY SECONDS=` +
          `${Math.round((bestTf.barMinutes * 60))} to switch, and expect far fewer, longer trades.`);
  }

  // The change that matters most to how the bot feels to use: does waiting and
  // picking the best of ten beat reacting to whichever fires first?
  let selBlock = '';
  if (selectionStudy.grid.length) {
    const top = [...selectionStudy.grid].sort((a, b) => b.rPerDay - a.rPerDay).slice(0, 5);
    const fmt = (g) => `${g.key}: ${g.tradesPerDay.toFixed(1)}/day, ` +
      `${g.expectancyR >= 0 ? '+' : ''}${g.expectancyR.toFixed(2)}R each, ` +
      `${g.rPerDay >= 0 ? '+' : ''}${g.rPerDay.toFixed(2)}R/day, ` +
      `quiet ${g.quietPct.toFixed(0)}% of rounds`;

    selBlock = '\n\nWaiting and picking the best of all ten\n' +
      `Best five of ${selectionStudy.grid.length} schedules tried:\n` +
      top.map(fmt).join('\n');

    if (selectionStudy.baseline) {
      selBlock += `\n\nFor comparison, reacting every bar with no extra ` +
        `selectivity:\n${fmt(selectionStudy.baseline)}`;
    }
    if (selectionStudy.live) {
      selBlock += `\n\nWhat is configured to run now:\n${fmt(selectionStudy.live)}`;
    }
    const o = selectionStudy.oos;
    if (o && o.secondHalf && o.baselineSecondHalf) {
      selBlock += `\n\nChosen on the first half: ${o.chosenOnFirstHalf}\n` +
        `On the second half it never saw: ` +
        `${o.secondHalf.rPerDay >= 0 ? '+' : ''}${o.secondHalf.rPerDay.toFixed(2)}R/day ` +
        `against ${o.baselineSecondHalf.rPerDay >= 0 ? '+' : ''}` +
        `${o.baselineSecondHalf.rPerDay.toFixed(2)}R/day for reacting every bar.\n` +
        (o.verdict
          ? `Difference ${o.verdict.diffRPerDay >= 0 ? '+' : ''}` +
            `${o.verdict.diffRPerDay.toFixed(2)}R/day, t ` +
            `${o.verdict.t == null ? 'n/a' : o.verdict.t.toFixed(1)} (2.0 needed) — ` +
            `${o.verdict.label}.\n` +
            (o.verdict.beats
              ? 'Waiting and choosing genuinely earns more, not just fewer alerts.'
              : o.verdict.label === 'WORSE'
                ? 'Waiting costs money. Fewer alerts are still worth something, but ' +
                  'know that you are paying for them.'
                : 'On the money, waiting and reacting are indistinguishable. The case ' +
                  'for waiting is that it gives far fewer alerts for the same result, ' +
                  'which is a fair reason to prefer it - just not a profit one.')
          : 'Not enough second-half trades to judge.');
    }
  }

  sendTelegram(
    `Backtest — ${backtest.days.toFixed(0)} days of real Deriv candles, ` +
    `${activeSymbols.length} symbols, threshold ${CONFIDENCE_THRESHOLD}%\n\n` +
    `Current settings (SL ${SL_ATR_MULT}x / TP ${TP_ATR_MULT}x):\n` +
    `Signals ${active.perDay.toFixed(1)}/day · win ${active.winRate.toFixed(0)}% · ` +
    `${active.expectancy >= 0 ? '+' : ''}${active.expectancy.toFixed(2)}R per trade\n` +
    `Typical hold ${humanDuration(active.hold.median)} ` +
    `(${humanDuration(active.hold.p25)} to ${humanDuration(active.hold.p75)})\n\n` +
    `Stop and target comparison:\n${rows}\n\n${mgmtRows}${costBlock}${confBlock}` +
    `${thBlock}${selBlock}${tfBlock}${volBlock}${tsBlock}${oosBlock}\n\n${advice}`
  );

  console.log(`[BACKTEST] active ${ACTIVE_VARIANT}: ${active.perDay.toFixed(2)}/day, ` +
    `win ${active.winRate.toFixed(1)}%, exp ${active.expectancy.toFixed(3)}R | best ${best.key}`);
}

// ---- Structure scan ----
// Rather than guessing at another indicator combination, this asks the prior
// question: does ANY simple "look back L bars, hold H bars" rule have an edge on
// this instrument? It scans 144 of them in both directions and reports what
// survives. Two details make it honest rather than flattering:
//   - forward windows never overlap, because overlapping windows share the same
//     moves and inflate significance, which is how naive scans find edges in noise
//   - the bar a result must clear rises with the number of rules tried, since
//     testing 144 things guarantees some look good by luck
// Calibrated against 25 simulated random walks: zero false survivors.
const scanStats = { done: 0, expected: 0, perSymbol: [], reported: false };
// Instruments analysed but never traded, keyed by the code Deriv accepts.
const scanOnly = {};

function meanOf(xs) { return xs.reduce((a, b) => a + b, 0) / (xs.length || 1); }

function sdOf(xs) {
  if (xs.length < 2) return 0;
  const m = meanOf(xs);
  return Math.sqrt(xs.reduce((a, b) => a + (b - m) ** 2, 0) / (xs.length - 1));
}

function autocorrOf(r, lag) {
  const n = r.length - lag;
  if (n < 30) return null;
  const a = r.slice(0, n), b = r.slice(lag, lag + n);
  const ma = meanOf(a), mb = meanOf(b);
  let num = 0, da = 0, db = 0;
  for (let i = 0; i < n; i++) {
    const x = a[i] - ma, y = b[i] - mb;
    num += x * y; da += x * x; db += y * y;
  }
  return da && db ? num / Math.sqrt(da * db) : null;
}

// A random walk's variance grows linearly with horizon, so this sits at 1.0.
// Above 1 means moves extend; below 1 means they get partly undone.
function varianceRatioOf(r, q) {
  if (r.length < q * 30) return null;
  const v1 = sdOf(r) ** 2;
  const agg = [];
  for (let i = 0; i + q <= r.length; i += q) agg.push(r.slice(i, i + q).reduce((a, b) => a + b, 0));
  const vq = sdOf(agg) ** 2;
  return v1 > 0 ? vq / (q * v1) : null;
}

// Bonferroni-style threshold: the largest |t| you should expect from pure noise
// after looking at this many rules.
function significanceBar(tests) {
  const p = 1 - 0.05 / (2 * Math.max(tests, 1));
  const a = [-39.6968302866538, 220.946098424521, -275.928510446969,
    138.357751867269, -30.6647980661472, 2.50662827745924];
  const b = [-54.4760987982241, 161.585836858041, -155.698979859887,
    66.8013118877197, -13.2806815528857];
  const c = [-0.00778489400243029, -0.322396458041136, -2.40075827716184,
    -2.54973253934373, 4.37466414146497, 2.93816398269878];
  const d = [0.00778469570904146, 0.32246712907004, 2.445134137143, 3.75440866190742];
  // The central rational approximation is only valid for p below about 0.976.
  // Correcting for many tests pushes p far into the upper tail, so the tail
  // branch is not optional: without it this returned 3.15 where the true value
  // is 3.47, understating the bar and letting noise register as a finding.
  const pl = 0.02425;
  if (p > 1 - pl) {
    const q = Math.sqrt(-2 * Math.log(1 - p));
    return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
      ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  }
  const q = p - 0.5, rr = q * q;
  return (((((a[0] * rr + a[1]) * rr + a[2]) * rr + a[3]) * rr + a[4]) * rr + a[5]) * q /
    (((((b[0] * rr + b[1]) * rr + b[2]) * rr + b[3]) * rr + b[4]) * rr + 1);
}

function structureScan(label, candles, kind = 'synthetic') {
  const closes = candles.map((c) => c.close);
  const r = [];
  for (let i = 1; i < closes.length; i++) r.push(closes[i] - closes[i - 1]);
  const scale = sdOf(r) || 1;

  // 1 to 12 candles covers up to three hours. The longer values extend the reach
  // to eight hours, which is as far as the data honestly allows: forward windows
  // must not overlap, so a 32-candle hold needs 32 x 120 candles to produce a
  // usable sample, and Deriv caps history at 5000 per request.
  const SPANS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 20, 24, 32];
  const grid = [];
  for (const L of SPANS) {
    for (const H of SPANS) {
      const payoffs = [];
      for (let i = L; i + H <= r.length; i += H) {
        const past = r.slice(i - L, i).reduce((a, b) => a + b, 0);
        if (past === 0) continue;
        payoffs.push(Math.sign(past) * r.slice(i, i + H).reduce((a, b) => a + b, 0));
      }
      if (payoffs.length < 120) continue;
      const m = meanOf(payoffs), s = sdOf(payoffs);
      if (!(s > 0)) continue;
      grid.push({ L, H, n: payoffs.length, payoff: m / scale, t: m / (s / Math.sqrt(payoffs.length)) });
    }
  }
  if (!grid.length) return;

  const bar = significanceBar(grid.length);
  const best = grid.reduce((a, b) => (Math.abs(b.t) > Math.abs(a.t) ? b : a));
  const survivors = grid.filter((g) => Math.abs(g.t) > bar);

  scanStats.done += 1;
  scanStats.perSymbol.push({
    label, kind, bars: closes.length, tested: grid.length, bar, best, survivors,
    ac1: autocorrOf(r, 1), vr8: varianceRatioOf(r, 8),
  });
  console.log(`[SCAN] ${label} (${kind}): ${grid.length} rules, best L${best.L}/H${best.H} ` +
    `t=${best.t.toFixed(2)} (bar ${bar.toFixed(2)}), survivors ${survivors.length}`);
}

function reportStructureScan() {
  const target = scanStats.expected || activeSymbols.length;
  if (scanStats.reported || !target || scanStats.done < target) return;
  scanStats.reported = true;
  const all = scanStats.perSymbol;
  if (!all.length) return;

  // Each symbol's own bar corrects for the 96 rules tried on that symbol. But
  // ten symbols were scanned, so the real number of looks is ten times larger
  // and the bar that matters is the family-wide one. Judging per-symbol would
  // mean roughly one false finding every run, purely by chance.
  const totalTests = all.reduce((n, s) => n + s.tested, 0);
  const familyBar = significanceBar(totalTests);
  for (const s of all) s.familySurvivors = s.survivors.filter((g) => Math.abs(g.t) > familyBar);
  const withEdge = all.filter((s) => s.familySurvivors.length);
  const lines = all
    .sort((a, b) => Math.abs(b.best.t) - Math.abs(a.best.t))
    .slice(0, 5)
    .map((s) => `${s.label} (${s.kind}): best look ${s.best.L} hold ${s.best.H}, ` +
      `t ${s.best.t.toFixed(2)}, ${s.best.payoff >= 0 ? 'momentum' : 'reversion'}`);

  // The comparison this whole exercise exists for: do instruments generated by a
  // random number generator behave differently from instruments moved by real
  // money? If both come back empty, the strategy is the problem. If only the
  // synthetics do, the instrument is.
  const synth = all.filter((s) => s.kind === 'synthetic');
  const real = all.filter((s) => s.kind !== 'synthetic');
  const strongest = (set) => (set.length
    ? Math.max(...set.map((s) => Math.abs(s.best.t))).toFixed(2) : 'n/a');
  const survivorsIn = (set) => set.filter((s) => s.familySurvivors.length).length;

  let comparison = '';
  if (real.length) {
    comparison = `\n\nSynthetic vs real markets\n` +
      `Synthetic (${synth.length}): strongest t ${strongest(synth)}, ` +
      `${survivorsIn(synth)} with a surviving rule\n` +
      `Real markets (${real.length}): strongest t ${strongest(real)}, ` +
      `${survivorsIn(real)} with a surviving rule\n` +
      (survivorsIn(real) > survivorsIn(synth)
        ? 'Real markets show structure the synthetics do not. That is the difference ' +
          'between a price with something behind it and a price from a generator.'
        : survivorsIn(synth) === 0 && survivorsIn(real) === 0
          ? 'Neither shows anything at these horizons. Short-term direction is close to ' +
            'unpredictable on real markets too - the difference is that on synthetics it ' +
            'is unpredictable by construction, so no amount of searching would help.'
          : 'Mixed. Treat any single survivor with suspicion until it repeats.');
  }

  const momentum = all.filter((s) => s.best.payoff > 0).length;
  const verdict = withEdge.length
    ? `${withEdge.map((s) => s.label).join(', ')} clear even the family-wide bar. ` +
      'That is a real measured edge - but confirm it holds on a later stretch of ' +
      'history before trading it, because a rule found by scanning can still be luck.'
    : 'Nothing clears the bar anywhere. Direction is not predictable from recent ' +
      'direction at these horizons, so swapping in other indicators of the same kind ' +
      'will not change it. Any edge has to come from somewhere other than reading ' +
      'recent price direction.';

  sendTelegram(
    `Entry-logic scan — ${all[0].tested} rules on each of ${all.length} instruments ` +
    `(${totalTests} tests)\n\n` +
    `${lines.join('\n')}\n\n` +
    `Bar to clear: ${familyBar.toFixed(2)} across everything ` +
    `(${all[0].bar.toFixed(2)} within one instrument). Best anywhere: ` +
    `${strongest(all)}.\n` +
    `Momentum favoured on ${momentum} of ${all.length}, reversion on ` +
    `${all.length - momentum}.${comparison}\n\n${verdict}`
  );
  console.log(`[SCAN] symbols with surviving rules: ${withEdge.length}/${all.length}`);
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
  // Sample size is not a formality here. With around 30 gaps the hazard estimate
  // is noisy enough to report an edge on a series that is provably memoryless,
  // which is exactly the mistake that costs money. 80 gaps is where the
  // false-positive rate drops to roughly nothing in simulation.
  const enough = gaps.length >= SPIKE_MIN_GAPS;
  const verdict = !enough ? 'INSUFFICIENT'
    : (cv < 0.75 && ratio > 1.2) ? 'EDGE'
    : (cv < 0.9 && ratio > 1.1) ? 'WEAK' : 'NO EDGE';
  return { usable: true, curve, earlyHazard: h1, lateHazard: h2, ratio, mean, sd, cv,
    minGap: Math.min(...gaps), n: gaps.length, verdict };
}

const spikeLab = {
  symbols: {}, pending: 0, reported: false,
  queue: [], timer: null, intervalMs: SPIKE_REQUEST_MS, rateLimitHits: 0,
};

function requestSpikeHistory(code, end) {
  ws.send(JSON.stringify({
    ticks_history: code, style: 'ticks', count: SPIKE_BATCH_SIZE, end: end || 'latest',
  }));
}

// Deriv rate-limits ticks_history, and firing four symbols in parallel trips it
// within seconds. Everything goes through one queue drained at a fixed interval
// so the pacing is global rather than per symbol, and the interval widens on its
// own if Deriv still objects.
function enqueueSpike(code, end, attempt = 0) {
  spikeLab.queue.push({ code, end, attempt });
  pumpSpikeQueue();
}

function pumpSpikeQueue() {
  if (spikeLab.timer) return;
  spikeLab.timer = setInterval(() => {
    const job = spikeLab.queue.shift();
    if (!job) {
      clearInterval(spikeLab.timer);
      spikeLab.timer = null;
      return;
    }
    requestSpikeHistory(job.code, job.end);
    spikeLab.inFlight = job;
  }, spikeLab.intervalMs);
}

function restartSpikeQueue() {
  if (spikeLab.timer) { clearInterval(spikeLab.timer); spikeLab.timer = null; }
  pumpSpikeQueue();
}

// Called when Deriv rejects a tick request. Without this the symbol would sit
// pending forever and the report would never fire.
function onSpikeError(code, errorCode) {
  const st = spikeLab.symbols[code];
  if (!st || st.done) return;
  const job = spikeLab.inFlight;
  const attempt = (job && job.code === code ? job.attempt : 0) + 1;

  if (errorCode === 'RateLimit' && attempt <= SPIKE_MAX_RETRIES) {
    spikeLab.rateLimitHits += 1;
    spikeLab.intervalMs = Math.min(15000, Math.round(spikeLab.intervalMs * 1.8));
    restartSpikeQueue();
    console.log(`[SPIKE] ${st.label} rate limited, slowing to ${spikeLab.intervalMs}ms ` +
      `(attempt ${attempt}/${SPIKE_MAX_RETRIES})`);
    enqueueSpike(code, st.oldest === null ? undefined : st.oldest - 1, attempt);
    return;
  }

  // Out of retries, or a different error entirely: finish with whatever this
  // symbol managed to collect rather than hanging the whole report.
  console.error(`[SPIKE] ${st.label} giving up after ${st.batches} batches (${errorCode})`);
  finishSpikeSymbol(code);
}

function startSpikeLab(list) {
  // Set SPIKE_BATCHES to 0 once the spike question is settled. It costs several
  // hundred requests per restart, and running it alongside candle pagination is
  // what makes the two collectively trip Deriv's rate limiter.
  if (SPIKE_BATCHES < 1) {
    console.log('[SPIKE] disabled (SPIKE_BATCHES=0)');
    return;
  }
  for (const s of list) {
    const meta = SPIKE_META[s.label];
    if (!meta) continue;
    spikeLab.symbols[s.code] = {
      label: s.label, meta, quotes: [], batches: 0, oldest: null, done: false,
    };
    spikeLab.pending += 1;
    enqueueSpike(s.code);
  }
  if (spikeLab.pending) {
    console.log(`[SPIKE] queued ${SPIKE_BATCHES} x ${SPIKE_BATCH_SIZE} ticks for ` +
      Object.values(spikeLab.symbols).map((s) => s.label).join(', ') +
      ` at ${spikeLab.intervalMs}ms per request`);
  }
}

// Deriv caps a single request at 5000 ticks, so history is walked backwards one
// batch at a time using the oldest timestamp seen so far.
function onSpikeHistory(code, prices, times) {
  const st = spikeLab.symbols[code];
  if (!st || st.done) return;
  const newOldest = times[0];
  st.quotes = prices.map(Number).concat(st.quotes);
  st.batches += 1;

  // Keep walking backwards while batches still arrive and the window is
  // genuinely moving into older data. The timestamp guard matters: if Deriv runs
  // out of history it returns the same window forever, which would loop.
  const movedBack = st.oldest === null || newOldest < st.oldest;
  st.oldest = newOldest;
  if (st.batches < SPIKE_BATCHES && prices.length >= 100 && movedBack) {
    enqueueSpike(code, newOldest - 1);
    return;
  }

  finishSpikeSymbol(code);
}

function finishSpikeSymbol(code) {
  const st = spikeLab.symbols[code];
  if (!st || st.done) return;
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

  // "Not enough data" is not the same as "no edge", and neither is it evidence
  // of one. Conflating them would be the most misleading thing this report could
  // do, so all three outcomes get their own conclusion.
  const strong = all.filter((s) => s.result.usable && s.result.verdict === 'EDGE');
  const weak = all.filter((s) => s.result.usable && s.result.verdict === 'WEAK');
  const judged = all.filter((s) => s.result.usable && s.result.verdict !== 'INSUFFICIENT');

  // In simulation this test never once called a memoryless series EDGE, but it
  // did return WEAK on about 7% of them. So WEAK is reported as unresolved
  // rather than as a finding - acting on it would be acting on noise.
  const conclusion = strong.length
    ? `${strong.map((s) => s.label).join(', ')} show a spike probability that genuinely rises ` +
      'as you wait. That is the one place a real edge could live here - ask for entry rules next.'
    : weak.length
      ? `${weak.map((s) => s.label).join(', ')} came out borderline. Roughly one in fourteen ` +
        'purely random runs looks like this, so it is not yet a finding. Collect more history ' +
        'before treating it as one.'
      : judged.length
        ? 'Every symbol with enough data looks memoryless: the chance of a spike on the next ' +
          'tick does not rise as you wait. Counting ticks cannot produce an edge here, and any ' +
          'bot claiming otherwise is selling you a coin flip.'
        : 'Not enough spikes collected to judge either way. That is missing data, not evidence ' +
          `of no edge. Raise SPIKE BATCHES above ${SPIKE_BATCHES} and restart to gather more.`;

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

// Reads the current state of one symbol without deciding anything. Split out so
// selection mode can score all ten at the same instant and compare them, which
// is impossible when scoring and alerting are the same step.
function assessSymbol(code) {
  const st = state[code];
  if (!st || st.candles.length < 55) return null;
  const closes = st.candles.map((c) => c.close);
  const ema20Series = ema(closes, 20);
  const ema50Series = ema(closes, 50);
  const rsiVal = rsi(closes, 14);
  const atrVal = atr(st.candles, 14);
  const price = closes.at(-1);

  if (rsiVal === null || !Number.isFinite(price)) return null;
  // Every level below is an ATR multiple, so without a usable ATR there is no
  // signal to send — better to stay quiet than to publish a stop of zero.
  if (!atrVal || !Number.isFinite(atrVal) || atrVal <= 0) return null;

  const { confidence, direction, parts } = scoreSignal({
    closes, ema20Series, ema50Series, rsiVal, atrVal, price,
  });
  return { confidence, direction, parts, rsiVal, atrVal, price, ema20Series, ema50Series };
}

// Announces the daily stop once, so the quiet is explained rather than mistaken
// for the bot having died.
function announceDailyStopIfNeeded() {
  if (dailyLimitHit() && !daily.announced) {
    daily.announced = true;
    sendTelegram(
      `Daily loss limit reached: ${daily.r.toFixed(1)}R today ` +
      `(limit ${DAILY_LOSS_LIMIT_R}R).\n\n` +
      'No new signals until the UTC day rolls over. Open trades are still tracked ' +
      'and will report as normal.'
    );
    console.log(`[RISK] daily limit hit at ${daily.r.toFixed(2)}R, pausing new entries`);
  }
}

// Sends the instruction and opens the tracked position. The decision to trade
// has already been made by the time this runs.
function emitEntry(code, label, reading, note = '') {
  const st = state[code];
  const { confidence, direction, parts, rsiVal, atrVal, price, ema20Series, ema50Series } = reading;
  const { entry, sl, tp, risk, reward } = levelsFor(direction, price, atrVal);
  const d = st.decimals ?? 2;
  const arrow = direction === 'BUY' ? '\u{1F7E2}' : '\u{1F534}';
  const g = holdGuidance();

  sendTelegram(
    `${arrow} *${direction} ${label} NOW*\n\n` +
    `Entry  ${entry.toFixed(d)}\n` +
    `SL     ${sl.toFixed(d)}  (${direction === 'BUY' ? '-' : '+'}${risk.toFixed(d)})\n` +
    `TP     ${tp.toFixed(d)}  (${direction === 'BUY' ? '+' : '-'}${reward.toFixed(d)})\n` +
    `R:R    ${rrLabel}\n` +
    `${positionSizeLine(risk, d)}\n\n` +
    `Hold   about ${humanDuration(g.median)}, usually ${humanDuration(g.p25)} to ${humanDuration(g.p75)}\n` +
    `Close  after ${humanDuration(TIME_STOP_BARS)} if neither level is hit\n` +
    (BREAKEVEN_AT_R > 0
      ? `Stop   moves to entry at +${BREAKEVEN_AT_R}R, then trails ${TRAIL_DISTANCE_R}R behind\n`
      : '') +
    '\n' +
    `*Confidence ${confidence}%*\n` +
    (note ? `${note}\n` : '') +
    `RSI(14) ${rsiVal.toFixed(1)} · ATR(14) ${atrVal.toFixed(d)}\n` +
    // Printed so the numbers can be checked against an M15 chart directly,
    // rather than judged by eye from where two lines appear to sit.
    `EMA20 ${ema20Series.at(-1).toFixed(d)} · EMA50 ${ema50Series.at(-1).toFixed(d)} ` +
    `(${ema20Series.at(-1) > ema50Series.at(-1) ? 'EMA20 above' : 'EMA20 below'})\n` +
    `Check on M15, EMA 20 and 50 on Close`,
    { markdown: true }
  );

  st.open = {
    direction, entry, sl, tp, risk, confidence,
    bars: 0, openedAt: Date.now(), openBarTime: st.candles.at(-1).time,
  };
  rollDay();
  daily.entries += 1;
  console.log(
    `[ALERT] ${label} ${direction} @ ${confidence}% ` +
    `entry=${entry.toFixed(d)} sl=${sl.toFixed(d)} tp=${tp.toFixed(d)} ` +
    `parts=${JSON.stringify(parts)}`
  );
  st.lastAlertAt = Date.now();
  st.lastSignal = { direction, confidence, entry, sl, tp, at: Date.now() };
}

function evaluateSignal(code, label) {
  const st = state[code];
  const reading = assessSymbol(code);
  if (!reading) return;
  const { confidence, direction } = reading;

  // In selection mode nothing is triggered here. The scores are kept up to date
  // and the scheduled round decides, so a symbol crossing its threshold at an
  // awkward moment cannot take the slot from a better one twenty minutes later.
  if (SELECTION_MODE) {
    st.lastDirection = direction;
    st.lastConfidence = confidence;
    return;
  }

  const bar = thresholdFor(label);
  const strong = confidence >= bar;
  const crossedUp = strong && st.lastConfidence < bar;
  const flipped = strong && st.lastDirection !== null && st.lastDirection !== direction;
  const cooledDown = Date.now() - st.lastAlertAt > ALERT_COOLDOWN_MS;
  // One position per symbol. Stacking a second call on V75 while the first is
  // still open doubles the risk on a single instrument and is the main reason
  // the old build felt noisy — the same trend kept re-announcing itself.
  const free = !st.open;
  const openCount = Object.values(state).filter((s) => s.open).length;
  const roomToTrade = openCount < MAX_OPEN_TRADES;
  const budgetLeft = !dailyLimitHit();

  announceDailyStopIfNeeded();

  if (free && budgetLeft && roomToTrade && (crossedUp || flipped) && cooledDown) {
    emitEntry(code, label, reading);
  }

  st.lastDirection = direction;
  st.lastConfidence = confidence;
}

// ---- Scheduled decision round ----
// Runs every DECISION_INTERVAL_MINUTES. Scores every symbol at the same instant,
// keeps only those clearing the floor, ranks them, and takes the strongest few.
// The whole point is that it is allowed to take nothing.
const decisionLog = { rounds: 0, taken: 0, empty: 0, last: null };

function humanInterval(minutes) {
  if (minutes % 1440 === 0) return `${minutes / 1440} day${minutes === 1440 ? '' : 's'}`;
  if (minutes % 60 === 0) return `${minutes / 60} hour${minutes === 60 ? '' : 's'}`;
  return `${minutes} minutes`;
}

function runDecisionRound() {
  rollDay();
  announceDailyStopIfNeeded();

  const openCount = Object.values(state).filter((s) => s.open).length;
  const room = Math.min(MAX_OPEN_TRADES - openCount, DECISION_TOP_K);
  const blocked = dailyLimitHit() ? 'daily loss limit' : room <= 0 ? 'position cap' : null;

  const candidates = [];
  for (const code of activeSymbols) {
    const st = state[code];
    if (!st || st.open) continue;
    const reading = assessSymbol(code);
    if (!reading) continue;
    const label = labels[code] || code;
    // The floor is whichever is stricter: the round's floor, or the symbol's own
    // threshold. Boom and Crash keep their bump.
    const floor = Math.max(DECISION_FLOOR, thresholdFor(label));
    candidates.push({ code, label, reading, floor, qualifies: reading.confidence >= floor });
  }

  const qualified = candidates
    .filter((c) => c.qualifies)
    .sort((a, b) => b.reading.confidence - a.reading.confidence);

  const strongest = candidates.length
    ? candidates.reduce((a, b) => (b.reading.confidence > a.reading.confidence ? b : a))
    : null;

  decisionLog.rounds += 1;
  decisionLog.last = {
    at: Date.now(),
    scanned: candidates.length,
    qualified: qualified.length,
    best: strongest
      ? { label: strongest.label, confidence: strongest.reading.confidence, floor: strongest.floor }
      : null,
    blocked,
    took: [],
  };

  if (blocked || !qualified.length) {
    decisionLog.empty += 1;
    const why = blocked
      ? `held back by the ${blocked}`
      : candidates.length
        ? `best was ${decisionLog.last.best.label} at ${decisionLog.last.best.confidence}%, ` +
          `floor ${decisionLog.last.best.floor}%`
        : 'no symbol had enough data';
    console.log(`[DECISION] nothing taken — ${why}`);
    return;
  }

  const take = qualified.slice(0, Math.max(room, 0));
  for (const c of take) {
    const rank = qualified.length > 1
      ? `Best of ${qualified.length} qualifying across ${candidates.length} symbols`
      : `Only symbol clearing ${c.floor}% this round`;
    emitEntry(c.code, c.label, c.reading, rank);
    decisionLog.taken += 1;
    decisionLog.last.took.push({ label: c.label, confidence: c.reading.confidence });
  }
  console.log(`[DECISION] scanned ${candidates.length}, qualified ${qualified.length}, ` +
    `took ${take.map((c) => `${c.label}@${c.reading.confidence}`).join(', ')}`);
}

let decisionTimer = null;
function startDecisionSchedule() {
  if (!SELECTION_MODE || decisionTimer) return;
  const ms = DECISION_INTERVAL_MINUTES * 60 * 1000;

  // Align to the wall clock so rounds land at predictable times rather than
  // wherever the process happened to restart. A four-hour interval then means
  // 00:00, 04:00, 08:00 UTC and so on, which also makes the log readable.
  const now = Date.now();
  const firstDelay = ms - (now % ms);

  console.log(`[DECISION] selection mode on: every ${DECISION_INTERVAL_MINUTES}m, ` +
    `top ${DECISION_TOP_K}, floor ${DECISION_FLOOR}%. First round in ` +
    `${Math.round(firstDelay / 60000)}m.`);

  setTimeout(() => {
    // Give the candle history a moment to fill on a cold start; a round that
    // scores nothing is worse than a round three minutes late.
    runDecisionRound();
    decisionTimer = setInterval(runDecisionRound, ms);
  }, firstDelay);
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

  if (!outcome) {
    // Still open: advance the stop for subsequent bars. Announce the move to
    // break-even once, because it changes the trade from risking 1R to risking
    // nothing and that is worth knowing.
    const moved = manageStop({ ...o, dir: o.direction }, bar, LIVE_MGMT);
    if (moved !== o.sl) {
      const wasAtRisk = long ? o.sl < o.entry : o.sl > o.entry;
      const nowSafe = long ? moved >= o.entry : moved <= o.entry;
      o.sl = moved;
      if (wasAtRisk && nowSafe && !o.beAnnounced) {
        o.beAnnounced = true;
        sendTelegram(
          `🛡 *STOP TO BREAK EVEN* — ${label} ${o.direction}\n\n` +
          `Entry  ${o.entry.toFixed(st.decimals ?? 2)}\n` +
          `Stop   ${moved.toFixed(st.decimals ?? 2)}\n` +
          'This trade can no longer lose.',
          { markdown: true }
        );
      }
    }
    return;
  }

  const d = st.decimals ?? 2;
  const rMultiple = ((long ? exit - o.entry : o.entry - exit) / o.risk);
  const rTxt = `${rMultiple >= 0 ? '+' : ''}${rMultiple.toFixed(1)}R`;
  const held = `${o.bars} candle${o.bars === 1 ? '' : 's'} (${humanDuration(o.bars)})`;

  if (outcome === 'TP') outcomes.tp += 1;
  else if (outcome === 'SL') outcomes.sl += 1;
  else outcomes.timeout += 1;
  // Timed-out trades never reached a level, so they would skew the hold stats.
  if (outcome !== 'TIME') outcomes.resolvedBars.push(o.bars);
  recordDailyR(rMultiple);
  if (outcome === 'TP') daily.tp += 1;
  else if (outcome === 'SL') daily.sl += 1;
  else daily.timeout += 1;

  // A stop that has been trailed into profit is not a loss, and calling it one
  // would make the day's tally read wrong.
  const trailedOut = outcome === 'SL' && rMultiple >= 0;
  const header = outcome === 'TP' ? '✅ *TARGET HIT*'
    : trailedOut ? '🛡 *TRAILED OUT*'
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

// Picks a spread of real instruments and asks for their candles. No subscribe,
// so nothing streams and nothing can ever be traded from this list - the data
// arrives once, gets scanned, and is discarded.
function startOtherMarketScan(available) {
  scanStats.expected = activeSymbols.length;
  if (!SCAN_OTHER_MARKETS) return;

  const others = available
    .filter((s) => s.market !== 'synthetic_index')
    .filter((s) => s.underlying_symbol && !scanOnly[s.underlying_symbol])
    .slice(0, SCAN_OTHER_LIMIT);
  if (!others.length) return;

  for (const s of others) {
    scanOnly[s.underlying_symbol] = {
      label: s.underlying_symbol_name || s.underlying_symbol,
      market: s.market || 'market',
    };
  }
  scanStats.expected += others.length;

  // Through the same throttled queue as the pagination, so the two cannot
  // collectively trip the rate limiter.
  for (const s of others) enqueueHistory({ code: s.underlying_symbol });

  console.log(`[SCAN] also analysing ${others.length} non-synthetic instruments: ` +
    others.map((s) => s.underlying_symbol).join(', '));
}

// ---- Candle history collection ----
// One throttled queue for every extra history request, so pagination and the
// other-market scan cannot collectively trip Deriv's rate limiter. Mirrors the
// spike collector: fixed pacing, widening automatically if Deriv objects.
const histLab = {
  symbols: {}, queue: [], timer: null, inFlight: null,
  intervalMs: HISTORY_REQUEST_MS, rateLimitHits: 0,
};

function enqueueHistory(job) {
  histLab.queue.push({ attempt: 0, ...job });
  pumpHistoryQueue();
}

function pumpHistoryQueue() {
  if (histLab.timer) return;
  histLab.timer = setInterval(() => {
    const job = histLab.queue.shift();
    if (!job) { clearInterval(histLab.timer); histLab.timer = null; return; }
    histLab.inFlight = job;
    ws.send(JSON.stringify({
      ticks_history: job.code, style: 'candles', granularity: GRANULARITY,
      count: HISTORY_BARS, end: job.end || 'latest',
    }));
  }, histLab.intervalMs);
}

function onHistoryError(code, errorCode) {
  const job = histLab.inFlight;
  const attempt = (job && job.code === code ? job.attempt : 0) + 1;
  const st = histLab.symbols[code];

  if (errorCode === 'RateLimit' && attempt <= HISTORY_MAX_RETRIES) {
    histLab.rateLimitHits += 1;
    histLab.intervalMs = Math.min(15000, Math.round(histLab.intervalMs * 1.8));
    if (histLab.timer) { clearInterval(histLab.timer); histLab.timer = null; }
    pumpHistoryQueue();
    console.log(`[HIST] rate limited, slowing to ${histLab.intervalMs}ms ` +
      `(attempt ${attempt}/${HISTORY_MAX_RETRIES})`);
    enqueueHistory({ ...job, attempt });
    return;
  }
  console.error(`[HIST] giving up on ${code} (${errorCode})`);
  if (st && !st.done) finishHistory(code);
}

// Called with the first, subscribed batch. Everything after this is pagination.
function beginHistory(code, label, candles) {
  histLab.symbols[code] = {
    label, buffer: candles.slice(), batches: 1, oldest: candles[0]?.time ?? null, done: false,
  };
  enqueueHistory({ code, end: (candles[0]?.time ?? 0) - 1 });
}

function onHistoryPage(code, candles) {
  const st = histLab.symbols[code];
  if (!st || st.done) return;
  const newOldest = candles[0]?.time ?? null;
  const movedBack = newOldest !== null && (st.oldest === null || newOldest < st.oldest);
  if (candles.length) st.buffer = candles.concat(st.buffer);
  st.batches += 1;
  st.oldest = newOldest ?? st.oldest;

  if (st.batches < HISTORY_BATCHES && candles.length >= 100 && movedBack) {
    enqueueHistory({ code, end: newOldest - 1 });
    return;
  }
  finishHistory(code);
}

function finishHistory(code) {
  const st = histLab.symbols[code];
  if (!st || st.done) return;
  st.done = true;
  const days = st.buffer.length
    ? (st.buffer.at(-1).time - st.buffer[0].time) / 86400 : 0;
  console.log(`[HIST] ${st.label}: ${st.buffer.length} candles over ${days.toFixed(1)} days ` +
    `(${st.batches} requests)`);
  backtestSymbol(code, st.label, st.buffer);
  structureScan(st.label, st.buffer, 'synthetic');
  st.buffer = [];
  reportBacktest();
  reportStructureScan();
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
      const reqCode = data.echo_req?.ticks_history;
      // A failed tick request belongs to the spike lab and must be retried or
      // retired, otherwise that symbol stays pending and the report never fires.
      if (reqCode && spikeLab.symbols[reqCode] && data.echo_req?.style === 'ticks') {
        onSpikeError(reqCode, data.error.code);
        return;
      }
      // A failed history page must be retried or retired, or that symbol never
      // finishes and the backtest never fires.
      if (reqCode && data.echo_req?.style === 'candles' && histLab.symbols[reqCode]) {
        onHistoryError(reqCode, data.error.code);
        return;
      }
      console.error('Deriv error:', data.error.code, data.error.message,
        '| requested symbol:', reqCode);
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
        (SELECTION_MODE
          ? `\n\nSelection mode: every ${humanInterval(DECISION_INTERVAL_MINUTES)} the bot ` +
            `scores all ${activeSymbols.length} symbols at once and takes ` +
            `${DECISION_TOP_K === 1 ? 'only the strongest' : `the strongest ${DECISION_TOP_K}`}, ` +
            `and only above ${DECISION_FLOOR}% ` +
            `(${CONFIDENCE_THRESHOLD + SPIKE_THRESHOLD_BUMP}% on Boom/Crash).\n` +
            'If nothing clears the bar, it sends nothing. Quiet days are expected.'
          : `\nAlerting above ${CONFIDENCE_THRESHOLD}% confidence ` +
            `(${SPIKE_THRESHOLD_BUMP > 0 ? `${CONFIDENCE_THRESHOLD + SPIKE_THRESHOLD_BUMP}% on Boom/Crash` : 'same on all'})`) +
        `\nSL ${SL_ATR_MULT}x ATR / TP ${TP_ATR_MULT}x ATR (${rrLabel}).`
      );
      subscribe(activeSymbols);
      startSpikeLab(activeSymbols);
      startOtherMarketScan(all);
      startDecisionSchedule();
      return;
    }

    // Tick history comes back as msg_type 'history', separate from the candle
    // stream, and is used only for the Boom/Crash spike analysis.
    if (data.msg_type === 'history' && data.history && data.echo_req?.ticks_history) {
      onSpikeHistory(data.echo_req.ticks_history, data.history.prices, data.history.times);
      return;
    }

    const reqSymbol = data.echo_req?.ticks_history;

    // Analysis-only instruments: scanned once, never stored, never traded.
    if (data.msg_type === 'candles' && reqSymbol && scanOnly[reqSymbol]) {
      const meta = scanOnly[reqSymbol];
      structureScan(meta.label, data.candles.map((c) => ({
        time: c.epoch, open: +c.open, high: +c.high, low: +c.low, close: +c.close,
      })), meta.market);
      reportStructureScan();
      return;
    }

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
      // The subscribed response carries only the most recent 1000 candles, which
      // is ten days. The backtest waits until pagination has walked back through
      // several months, because a hundred trades cannot settle anything.
      //
      // Only the subscribed batch may touch the live candle window. Paginated
      // pages are older data, and letting them overwrite it would rewind the
      // monitor to last month and fire signals from stale prices.
      if (data.echo_req?.subscribe === 1) {
        st.candles = full.slice(-150);
        beginHistory(reqSymbol, labels[reqSymbol] || reqSymbol, full);
        evaluateSignal(reqSymbol, labels[reqSymbol] || reqSymbol);
      } else {
        onHistoryPage(reqSymbol, full);
      }
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
      perSymbolThresholds: SYMBOL_THRESHOLDS,
      risk: { slAtrMult: SL_ATR_MULT, tpAtrMult: TP_ATR_MULT, rr: rrLabel, timeStopBars: TIME_STOP_BARS },
      costs: {
        spreadAtrAssumed: SPREAD_ATR,
        costPerTradeR: +(SPREAD_ATR / SL_ATR_MULT).toFixed(4),
        rCostPer001Atr: +(0.01 / SL_ATR_MULT).toFixed(4),
      },
      selection: SELECTION_MODE ? {
        mode: 'scheduled ranking across all symbols',
        everyMinutes: DECISION_INTERVAL_MINUTES,
        takesTop: DECISION_TOP_K,
        floor: DECISION_FLOOR,
        roundsRun: decisionLog.rounds,
        entriesTaken: decisionLog.taken,
        roundsWithNothing: decisionLog.empty,
        lastRound: decisionLog.last,
      } : { mode: 'alerts on threshold cross, per symbol' },
      hold: holdGuidance(),
      backtest: {
        days: Math.round(backtest.days),
        signals: backtest.signals,
        perDay: +(backtest.signals / Math.max(backtest.days, 1)).toFixed(2),
        tp: backtest.tp, sl: backtest.sl, timeout: backtest.timeout,
      },
      liveOutcomes: { tp: outcomes.tp, sl: outcomes.sl, timeout: outcomes.timeout },
      // Every study reachable from one place, so a question never has to be
      // chased through chat history again.
      ...(() => {
        const digest = (store) => Object.fromEntries(Object.entries(store).map(([k, a]) => {
          const n = a.tp + a.sl + a.timeout;
          return [k, n ? { trades: n, winPct: +((100 * a.tp) / n).toFixed(1),
            expectancyR: +(a.sumR / n).toFixed(3) } : null];
        }));
        return {
          byConfidence: digest(backtest.byConfidence),
          byVolatility: digest(backtest.byVolatility),
          byTimeStop: digest(backtest.byTimeStop),
        };
      })(),
      thresholdStudy: (() => {
        const r = thresholdOutOfSample();
        const row = (s) => (s ? {
          trades: s.n, expectancyR: +s.exp.toFixed(3), winPct: +s.win.toFixed(1),
          t: s.t == null ? null : +s.t.toFixed(2),
        } : null);
        return {
          running: CONFIDENCE_THRESHOLD,
          fullHistory: Object.fromEntries(r.table.map((x) => [String(x.threshold), {
            trades: x.n, perDay: +x.perDay.toFixed(2),
            winPct: +x.win.toFixed(1), expectancyR: +x.exp.toFixed(3),
            t: x.t == null ? null : +x.t.toFixed(2),
          }])),
          chosenOnFirstHalf: r.pick ? r.pick.threshold : null,
          firstHalf: r.pick ? row(r.pick.s) : null,
          secondHalf: row(r.after),
          secondHalfAtRunningThreshold: row(r.live),
          // Positive diff with t above 2 is the only reading that justifies
          // changing the setting. Anything less is noise wearing a decimal point.
          beatsRunningSetting: r.beatsLive ? {
            diffR: +r.beatsLive.diff.toFixed(3),
            t: r.beatsLive.t == null ? null : +r.beatsLive.t.toFixed(2),
            verdict: r.beatsLive.t != null && r.beatsLive.t > 2 ? 'CHANGE JUSTIFIED'
              : r.beatsLive.t != null && r.beatsLive.t > 0 ? 'BETTER BUT UNPROVEN' : 'NO',
          } : null,
        };
      })(),
      timeframeStudy: timeframeStudy(),
      selectionStudy: selectionStudy.done ? (() => {
        const slim = (g) => (g ? {
          key: g.key ?? null, trades: g.trades,
          tradesPerDay: +g.tradesPerDay.toFixed(2),
          winPct: +g.winPct.toFixed(1),
          expectancyR: +g.expectancyR.toFixed(3),
          rPerDay: +g.rPerDay.toFixed(3),
          quietRoundsPct: +g.quietPct.toFixed(1),
          t: g.t == null ? null : +g.t.toFixed(2),
        } : null);
        return {
          best: [...selectionStudy.grid].sort((a, b) => b.rPerDay - a.rPerDay).slice(0, 8).map(slim),
          configuredNow: slim(selectionStudy.live),
          reactEveryBarBaseline: slim(selectionStudy.baseline),
          outOfSample: selectionStudy.oos ? {
            chosenOnFirstHalf: selectionStudy.oos.chosenOnFirstHalf,
            secondHalf: slim(selectionStudy.oos.secondHalf),
            baselineSecondHalf: slim(selectionStudy.oos.baselineSecondHalf),
            verdict: selectionStudy.oos.verdict ? {
              diffRPerDay: +selectionStudy.oos.verdict.diffRPerDay.toFixed(3),
              t: selectionStudy.oos.verdict.t == null ? null : +selectionStudy.oos.verdict.t.toFixed(2),
              label: selectionStudy.oos.verdict.label,
            } : null,
            beatsBaseline: selectionStudy.oos.beatsBaseline,
          } : null,
        };
      })() : null,
      outOfSample: (() => {
        const o = outOfSampleResult();
        if (!o) return null;
        return {
          chosenOnFirstHalf: o.pick.key,
          firstHalf: { trades: o.pick.s.n, expectancyR: +o.pick.s.exp.toFixed(3), winPct: +o.pick.s.win.toFixed(1) },
          secondHalf: o.after
            ? { trades: o.after.n, expectancyR: +o.after.exp.toFixed(3), winPct: +o.after.win.toFixed(1) }
            : null,
          heldUp: o.heldUp,
        };
      })(),
      today: {
        date: daily.day, entries: daily.entries, tp: daily.tp, sl: daily.sl,
        timeout: daily.timeout, netR: +daily.r.toFixed(2),
        dailyLimitR: DAILY_LOSS_LIMIT_R, paused: dailyLimitHit(),
      },
      // career only accumulates when a UTC day rolls over, so on its own it
      // silently excludes today - which made "today: 13 stops" sit next to
      // "sinceStart: 11 stops" and look like a counting error. Today is added
      // back here so this total always reconciles with liveOutcomes.
      sinceStart: (() => {
        const tp = career.tp + daily.tp;
        const sl = career.sl + daily.sl;
        const timeout = career.timeout + daily.timeout;
        const n = tp + sl + timeout;
        return {
          from: career.since,
          days: career.days + (daily.day ? 1 : 0),
          trades: n,
          tp, sl, timeout,
          winPct: n ? +((100 * tp) / n).toFixed(1) : null,
          netR: +(career.r + daily.r).toFixed(2),
          expectancyR: n ? +((career.r + daily.r) / n).toFixed(3) : null,
          note: 'includes today; resets when the process restarts',
        };
      })(),
      completedDaysOnly: {
        days: career.days, netR: +career.r.toFixed(2),
        tp: career.tp, sl: career.sl, timeout: career.timeout,
      },
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
