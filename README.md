# Synthetic Signal Desk — Telegram Monitor

Always-on Node.js service that watches live Deriv synthetic-index prices and
sends a Telegram trade instruction whenever a signal crosses its confidence
threshold. It does not place trades — every signal is executed by hand.

Live: `https://synthetic-signal-monitor.onrender.com`
Repo: `github.com/micmusonda17/synthetic-signal-monitor`

## What it does

- Connects to Deriv's public WebSocket feed (no login, no API key)
- Streams live M15 candles for 10 synthetic instruments
- Computes EMA 20/50, RSI(14) and ATR(14) on every update
- Sends a direct BUY/SELL instruction with entry, stop, target, position size
  and expected holding time
- Tracks each trade to its conclusion and reports the outcome
- Re-runs six self-checks on 208 days of real history at every startup
- Auto-reconnects if the feed drops
- Exposes everything it knows as JSON at `/` for inspection

## Signals

Each alert is an instruction, not a hint:

```
🟢 BUY V75 NOW

Entry  53181.88
SL     52673.25  (-508.63)
TP     54199.14  (+1017.26)
R:R    1:2
Size   risk 1.83 USD (1%) over 508.63 pts

Hold   about 45m, usually 15m to 1.3h
Close  after 6h if neither level is hit

Confidence 77%
RSI(14) 77.3 · ATR(14) 339.09
EMA20 53102.4 · EMA50 52890.1 (EMA20 above)
```

The EMA line exists so the same reading can be confirmed on MT5 before entering.
If MT5 disagrees, the timeframe or EMA settings differ — the bot works on M15
with EMA 20/50 on close.

Only one position is tracked per symbol at a time, so the same trend cannot
re-announce itself while you are still in it. When the trade resolves:

```
✅ TARGET HIT — V75 BUY

Entry  53181.88
Exit   54199.14
Result +2.0R · held 9 candles (2.3h)
```

Stops, targets and time stops each produce one of these, so message volume is
about two per trade rather than an open-ended stream.

## Everything is measured, nothing is assumed

On startup the bot paginates 208 days of real candles per symbol out of Deriv
(the feed caps at 1000 records per request, so it walks backwards in pages) and
replays the exact live rules over them — same score, same threshold, same
cooldown, same one-position rule, same time stop, same stop management.

Current reading, 2,323 trades over 208 days:

```
Signals: 2324 (11.2/day across all symbols)
Hit target: 848 · Hit stop: 1462 · Timed out: 13
Win rate: 36.5%
Expectancy: +0.101R per trade   (t ≈ 3.4)
```

At 1:2 reward-to-risk, break-even is a 33.3% win rate — losing two trades in
three is the designed behaviour, not a fault.

Six independent studies run alongside the backtest and are reported on the
health endpoint:

| Study | Question | Current answer |
|---|---|---|
| Structure scan | Does any simple lookback/hold rule have an edge? | No — 5,632 rules across 22 instruments, nothing survives multiple-testing correction |
| Spike timing | Can Boom/Crash spikes be timed by waiting? | No — three of four are statistically memoryless |
| Confidence study | Do higher scores produce better trades? | Suggestive ladder (0.042R → 0.342R) but t = 1.76, unproven |
| Volatility regime | Does trading into rising volatility help? | No — +0.108R vs +0.101R |
| Time-stop sweep | What time limit is best? | No difference — 6/12/24/48 candles all within 0.004R |
| Out-of-sample | Does the chosen setting survive unseen data? | Yes — "SL1 TP2 break-even at 0.5R" chosen on the first half held on the second |

The hold guidance printed in each alert uses real percentiles from this
backtest, and is replaced by the bot's own closed trades once it has 20 of them.

## Known unknown: spread

`SPREAD_ATR` is currently `0`, meaning all figures above are gross of trading
cost. At a 1× ATR stop, every 0.01 ATR of spread costs 0.010R per trade, so a
spread of 0.10 × ATR would erase the entire measured edge. Read the spread off
MT5, divide by the ATR shown in an alert, and set `SPREAD_ATR` to that number —
the backtest then reports net expectancy.

## Configuration

All behaviour is environment variables. No code change is needed to retune.

### Required

| Variable | Value |
|---|---|
| `TELEGRAM_TOKEN` | Bot API token from BotFather |
| `TELEGRAM_CHAT_ID` | Chat ID to send alerts to |

### Risk and sizing

| Variable | Default | Meaning |
|---|---|---|
| `ACCOUNT_BALANCE` | `0` | Set it and alerts show risk in currency instead of a percentage |
| `RISK_PERCENT` | `1` | Percent of balance risked per trade |
| `ACCOUNT_CURRENCY` | `USD` | Display only |
| `MAX_OPEN_TRADES` | `3` | Cap on concurrent positions across all symbols |
| `DAILY_LOSS_LIMIT_R` | `3` | Pause new entries after this many R lost in a UTC day |

### Trade levels

| Variable | Default | Meaning |
|---|---|---|
| `SL_ATR_MULT` | `1.5` | Stop distance in ATRs (**Render runs `1`**) |
| `TP_ATR_MULT` | `3.0` | Target distance in ATRs (**Render runs `2`**) |
| `BREAKEVEN_AT_R` | `0` (off) | Move stop to entry once this far in profit |
| `TRAIL_AFTER_R` | `0` (off) | Start trailing once this far in profit |
| `TRAIL_DISTANCE_R` | `1` | How far behind the high-water mark the trail sits |
| `TIME_STOP_BARS` | `24` | Candles (6h) before a stale trade is called |
| `SPREAD_ATR` | `0` | Spread as a fraction of ATR, charged inside the backtest |

### Signal selectivity

| Variable | Default | Meaning |
|---|---|---|
| `CONFIDENCE_THRESHOLD` | `74` | Raise for fewer signals |
| `SPIKE_THRESHOLD_BUMP` | `6` | Extra threshold applied to Boom/Crash, which are noisier |
| `SYMBOL_THRESHOLDS` | — | Per-symbol overrides, e.g. `V75:76,B500:82` |
| `TREND_DIV` | `3.0` | Raise for stricter trend requirement |
| `SLOPE_DIV` | `1.4` | Raise for stricter slope requirement |
| `ENTRY_DIV` | `2.5` | How far from EMA20 still counts as a clean entry |
| `ALERT_COOLDOWN_MINUTES` | `15` | Minimum gap between alerts on one symbol |
| `GRANULARITY_SECONDS` | `900` | Candle size (900 = M15) |

Confidence is scored from four volatility-normalised components — EMA separation
(0–35), EMA20 slope (0–30), RSI agreement (0–20) and entry proximity to EMA20
(0–15) — then mapped onto 30–95. The divisors are calibrated so a driftless
random walk, which is essentially what a Deriv volatility index is, clears the
default threshold on roughly 7% of candles, while a genuinely drifting series
clears it on about 93%.

### History and studies

| Variable | Default | Meaning |
|---|---|---|
| `HISTORY_BATCHES` | `20` | Pages of 1000 candles per symbol (20 ≈ 208 days on M15) |
| `HISTORY_REQUEST_MS` | `1500` | Pacing between history requests; widens automatically on rate limit |
| `HISTORY_MAX_RETRIES` | `6` | Give up on a symbol after this many failures |
| `SPIKE_BATCHES` | `80` | Tick batches per Boom/Crash symbol (**Render runs `0`** — the question is settled) |
| `SPIKE_MIN_GAPS` | `80` | Minimum spike intervals before the memorylessness test will rule either way |
| `SPIKE_MULT` | `8` | How many typical moves defines a spike |
| `SCAN_OTHER_MARKETS` | `true` | Also run the structure scan on forex and commodities as a control |
| `SCAN_OTHER_LIMIT` | `12` | How many non-synthetic instruments to scan |
| `MIN_HOLD_SAMPLES` | `20` | Closed trades before live stats replace backtest hold times |

## Deploy

Currently on Render as a **Web Service** (not a background worker — it serves a
health endpoint on `PORT`).

1. New → Web Service, connect the GitHub repo
2. Build command `npm install`, start command `npm start`
3. Add `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID` under Environment, plus any
   overrides from the tables above
4. Pushing to `main` redeploys automatically

Deploy logs should show "Connected to Deriv feed." and a Telegram message
confirming startup. Visiting the service URL returns the full JSON state.

### Local run

```
npm install
TELEGRAM_TOKEN=xxx TELEGRAM_CHAT_ID=xxx node index.js
```

## Notes

- The bot token was shared in a chat session — rotate it via BotFather
  (`/mybots` → your bot → API Token → Revoke) before going live, since anyone
  with the old token can also message through the bot.
- To change which symbols are watched, edit the `SYMBOLS` array in `index.js`.
- State is in memory only: a redeploy clears open-trade tracking and running
  totals.
- Nothing here is financial advice. These instruments carry real risk of loss.
