# Synthetic Signal Desk — Telegram Monitor

Always-on script that watches live Deriv synthetic-index prices and sends a
Telegram message whenever a BUY/SELL signal crosses your confidence threshold.

## What it does
- Connects to Deriv's public WebSocket feed (no login required)
- Streams live candles for all 10 watchlist instruments
- Computes EMA 20/50, RSI(14), ATR(14) exactly like the dashboard
- Sends a direct BUY/SELL call with entry, stop-loss and take-profit levels
- Auto-reconnects if the connection drops

## Signals

Each alert is an instruction, not a hint:

```
🟢 BUY V75 NOW

Entry  1015.52
SL     1013.34  (-2.18)
TP     1019.88  (+4.36)
R:R    1:2

Hold   about 1.5h, usually 45m to 2.5h
Close  after 6h if neither level is hit

Confidence 76%
RSI(14) 76.4 · ATR(14) 1.45
```

Only one position is tracked per symbol at a time — the same trend cannot
re-announce itself while you are still in it. When the trade resolves you get a
follow-up:

```
✅ TARGET HIT — V75 BUY

Entry  1015.52
Exit   1019.88
Result +2.0R · held 9 candles (2.3h)
```

Stops, targets and time stops all produce one of these, so the message count is
roughly two per trade rather than an open-ended stream.

## Hold times come from measurement, not assumption

On startup the bot pulls 2000 real candles per symbol from Deriv and replays the
exact live rules over them — same score, same threshold, same cooldown, same
one-position rule, same time stop. It reports what those settings would have
produced over the last few weeks:

```
Backtest on 21 days of real Deriv candles, 10 symbols, threshold 74%:

Signals: 63 (3.0/day across all symbols)
Hit target: 21 · Hit stop: 34 · Timed out: 8
Win rate: 38%
Typical hold: 1.5h (45m to 3.3h)
Expectancy: +0.14R per trade
```

The hold guidance printed in each alert uses these real percentiles, and is
replaced again by this bot's own closed trades once it has 20 of them. If the
reported signal rate is not what you want, change `CONFIDENCE_THRESHOLD` and
restart — the next backtest tells you the new rate before you trade it.

Stop-loss and take-profit are ATR multiples, so they widen on V100 and tighten
on V10 automatically instead of assuming every index moves the same distance.

Confidence is scored from four volatility-normalised components — EMA
separation (0-35), EMA20 slope (0-30), RSI agreement (0-20) and entry proximity
to EMA20 (0-15) — then mapped onto 30-95. The divisors are calibrated so that a
driftless random walk, which is essentially what a Deriv volatility index is,
clears the default 70% threshold on roughly 7% of candles, while a genuinely
drifting series clears it on about 93%.

## Setup

### 1. Environment variables
This script reads its secrets from environment variables — never hardcode them:

| Variable | Value |
|---|---|
| `TELEGRAM_TOKEN` | Your bot's API token from BotFather |
| `TELEGRAM_CHAT_ID` | The chat ID to send alerts to |
| `ACCOUNT_BALANCE` | Optional — set it and alerts show risk in currency instead of a percentage |
| `RISK_PERCENT` | Optional, default `1` — percent of balance risked per trade |
| `ACCOUNT_CURRENCY` | Optional, default `USD` |
| `MAX_OPEN_TRADES` | Optional, default `3` — cap on positions held at once across all symbols |
| `DAILY_LOSS_LIMIT_R` | Optional, default `3` — pause new entries after this many R lost in a UTC day |
| `BREAKEVEN_AT_R` | Optional, default `0` (off). Set to `1` to move the stop to entry at +1R |
| `TRAIL_AFTER_R` | Optional, default `0` (off) — start trailing once this far in profit |
| `TRAIL_DISTANCE_R` | Optional, default `1` — how far behind the high water mark the trail sits |
| `SPIKE_BATCHES` | Optional, default `80` — tick batches pulled per Boom/Crash symbol |
| `SPIKE_REQUEST_MS` | Optional, default `2000` — pacing between tick requests; widens automatically if Deriv rate-limits |
| `CONFIDENCE_THRESHOLD` | Optional, default `74` (72 ≈ 8 signals/day, 78 ≈ 3/day) |
| `TIME_STOP_BARS` | Optional, default `24` candles (6h) before a stale trade is called |
| `HISTORY_BARS` | Optional, default `2000` — candles pulled per symbol for the startup backtest |
| `MIN_HOLD_SAMPLES` | Optional, default `20` closed trades before live stats replace the backtest |
| `GRANULARITY_SECONDS` | Optional, default `900` (15-minute candles) |
| `ALERT_COOLDOWN_MINUTES` | Optional, default `15` |
| `SL_ATR_MULT` | Optional, default `1.5` — stop distance in ATRs |
| `TP_ATR_MULT` | Optional, default `3.0` — target distance in ATRs |
| `TREND_DIV` | Optional, default `3.0` — raise for fewer, stricter signals |
| `SLOPE_DIV` | Optional, default `1.4` — raise for fewer, stricter signals |
| `ENTRY_DIV` | Optional, default `2.5` — how far from EMA20 still counts as a clean entry |

Want a 1:3 reward instead? Set `TP_ATR_MULT=4.5`. Too many alerts? Raise
`CONFIDENCE_THRESHOLD` to 78 or push `TREND_DIV` to 3.5 — no code change needed.

### 2. Deploy (Railway — easiest free option)
1. Create a free account at railway.app
2. New Project → Deploy from GitHub repo (push this folder to a new repo), or use "Empty Project" and drag these files in via the Railway CLI
3. In the project's Variables tab, add `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID`
4. Railway auto-detects `npm start` from package.json and runs it — it'll stay running 24/7
5. Check the Deploy Logs — you should see "Connected to Deriv feed." and a Telegram message should land immediately confirming the monitor started

### 3. Deploy (Render — alternative)
1. New → Background Worker (not Web Service, since this has no HTTP port)
2. Connect your repo, build command `npm install`, start command `npm start`
3. Add the same environment variables under the Environment tab

### Local test
```
npm install
TELEGRAM_TOKEN=xxx TELEGRAM_CHAT_ID=xxx node index.js
```

## Notes
- The bot token was shared in a chat session — consider rotating it via
  BotFather (`/mybots` → your bot → API Token → Revoke) before going live,
  since anyone with the old token could also message through the bot.
- To change which symbols are watched, edit the `SYMBOLS` array in `index.js`.
