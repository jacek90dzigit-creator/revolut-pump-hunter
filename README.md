# Revolut Pump Hunter Android

Android app for monitoring public Revolut X market data without Revolut login/API keys.

## Features
- Pump Hunter: configurable % threshold and time window.
- Active Pumps: remembers detected pumps and tracks entry, peak, drawdown and momentum.
- Exit Signal: scores signs of a weakening move from 0-100 and alerts when the score reaches 70.
- Cooldown to reduce duplicate pump alerts.
- Local persistence of detected pumps.
- Android foreground service for continuous scanning.
- Mini in-app pump chart.

## Important data limitation
Public Revolut X endpoints are rate-limited. The scanner uses the public ticker endpoint for broad market scanning and builds local price history. OHLCV and public trade history are available from Revolut X, but querying them for every asset every minute would not fit the public endpoint rate limit. A future backend can enrich active-pump analysis with candle/trade volume and transaction-count features while respecting the API limits.

Exit Signal is an analytical warning, not a guaranteed sell signal or investment advice.

## Build
Run the included GitHub Actions workflow manually (`workflow_dispatch`) or build with Gradle/Android Studio. The workflow outputs `app-debug.apk`.
