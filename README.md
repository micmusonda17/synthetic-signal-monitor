# Synthetic Signal Desk — Telegram Monitor

Always-on script that watches live Deriv synthetic-index prices and sends a
Telegram message whenever a BUY/SELL signal crosses your confidence threshold.

## What it does
- Connects to Deriv's public WebSocket feed (no login required)
- Streams live candles for all 10 watchlist instruments
- Computes EMA 20/50, RSI(14), ATR(14) exactly like the dashboard
- Sends a Telegram message when a signal appears or flips direction
- Auto-reconnects if the connection drops

## Setup

### 1. Environment variables
This script reads its secrets from environment variables — never hardcode them:

| Variable | Value |
|---|---|
| `TELEGRAM_TOKEN` | Your bot's API token from BotFather |
| `TELEGRAM_CHAT_ID` | The chat ID to send alerts to |
| `CONFIDENCE_THRESHOLD` | Optional, default `70` |
| `GRANULARITY_SECONDS` | Optional, default `900` (15-minute candles) |

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
