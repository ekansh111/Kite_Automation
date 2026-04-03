# Kite API Detailed Operations Runbook

Date: 2026-04-01
Repository: Kite_API
Latest committed change set: `fbf15fe` (`Harden Angel remote execution and browser order flow`)

## 1. Purpose

This document is the detailed reference for the current trading-control setup discussed and implemented over the last week. It is intended to be an operations and engineering runbook, not just a changelog.

It covers:

- overall system design
- Unix and Windows runtime topology
- startup scripts and environment variables
- webhook payloads and endpoints
- Angel NCDEX design
- Angel web UI automation behavior
- quantity semantics
- commands to run
- test commands
- troubleshooting steps
- files changed during the workstream

## 2. High-Level Design

The current system is split across two machines:

- Unix EC2 is the main control plane
- Windows EC2 is the Angel browser execution worker

### 2.1 Unix EC2 responsibilities

- expose the public webhook listener
- receive TradingView webhooks on `/forecast`
- run the forecast orchestrator
- calculate forecasts, target positions, and deltas
- fetch Angel SmartAPI LTP for NCDEX orders when the webhook does not provide `LTP`
- forward Angel NCDEX execution requests to the Windows worker
- run reconciliation every 15 minutes
- expose `/status`

### 2.2 Windows EC2 responsibilities

- expose the internal Angel execution endpoint
- run the Angel Selenium browser worker
- maintain or launch the Chrome session used for Angel web trading
- log in to Angel web if needed
- add missing watchlist contracts if needed
- place the actual browser order in the Angel web UI

### 2.3 Broker split

- Zerodha orders remain local to the main control plane path
- Angel NCDEX orders are handled as a remote browser-execution flow

## 3. Primary Runtime Flow

### 3.1 TradingView to orchestrator

1. TradingView sends a JSON payload to `/forecast`.
2. `Server_Start.py` hands the payload to `ForecastOrchestrator.HandleWebhook(...)`.
3. The orchestrator:
   - validates the instrument
   - normalizes the subsystem name using `system_name_map`
   - converts `Netposition` into `+10 / 0 / -10`
   - logs the raw TradingView signal
   - upserts the derived forecast
   - pushes the instrument into its per-instrument queue
4. Flask returns immediately with HTTP 200 if the payload is accepted.
5. A background worker thread computes the new position and executes if needed.

### 3.2 Position calculation

For each instrument, the worker:

- combines subsystem forecasts using config weights
- applies forecast cap and scaling
- computes a raw desired position size
- rounds once to a target integer position
- compares target vs `confirmed_qty`
- decides whether an order is needed

### 3.3 Zerodha path

For Zerodha instruments:

- the orchestrator uses the existing Zerodha order flow
- if `execution.use_smart_chase=true`, smart chase is used
- smart chase places a limit order, monitors status, modifies if needed, and can fall back to market

### 3.4 Angel NCDEX path

For Angel NCDEX instruments:

1. The orchestrator builds an Angel order request.
2. If the webhook included `LTP`, Unix uses that price for the limit order.
3. If the webhook did not include `LTP`, Unix logs in to Angel SmartAPI and fetches the LTP first.
4. Unix resolves the exchange contract name.
5. Unix forwards the prepared order to the Windows worker through `/internal/angel-execute`.
6. Windows uses the browser bot to place the order in the Angel web UI.
7. Unix records the result or timeout.

### 3.5 Reconciliation

Every 15 minutes, Unix:

- fetches broker positions from Zerodha or Angel
- for NCDEX instruments, converts raw broker units to lots by dividing by `QuantityMultiplier` (e.g. broker returns 10 raw units for COCUDAKL → 10 / 10 = 1 lot)
- compares them with `system_positions.confirmed_qty` (stored in lots)
- syncs the DB to broker truth if needed
- logs mismatches
- sends reconciliation alert email when mismatches are found (quantities shown in lots, not raw units)

Important: "Reconciliation complete - all positions match" means the DB matches broker state. It does not mean positions are flat.

### 3.6 Rollover Monitor

`rollover_monitor.py` is a standalone process that runs daily (typically via cron).

- scans all open futures positions across Zerodha and Angel accounts
- resolves current and next month contracts from instrument CSVs
- evaluates rollover need based on trading days to expiry
- sends a **daily summary email every run** showing all positions with days-to-expiry, sorted by urgency:
  - red: within `alert_days_before_expiry` (rollover imminent)
  - amber: within `alert_days + 3` (approaching)
  - no color: far from expiry
- sends individual alert emails when positions enter the alert window
- executes two-leg rollovers (close current month, open next month) via SmartChaseExecute when within `execute_days_before_expiry`
- Angel NCDEX instruments stay alert-only (no auto-execution)
- logs all rollover attempts to SQLite for crash recovery

## 4. Key Files and Responsibilities

### 4.1 Runtime and routing

- `forecast_orchestrator.py`
  - queue-based orchestrator
  - webhook parsing
  - forecast combination
  - execution dispatch
  - reconciliation (converts NCDEX broker quantities from raw units to lots via `QuantityMultiplier`)
  - remote Angel execution handoff

- `rollover_monitor.py`
  - standalone daily rollover process
  - scans all positions across brokers
  - sends daily summary email with all position expiry statuses
  - executes two-leg rollovers via SmartChaseExecute
  - crash recovery for incomplete rollovers

- `Server_Start.py`
  - Flask entrypoint
  - route registration
  - executor-only mode
  - internal Angel auth check
  - `/forecast`, `/status`, `/internal/angel-execute`

- `Server_Order_Handler.py`
  - Angel SmartAPI login
  - Angel contract resolution
  - Angel LTP lookup
  - Angel browser route selection
  - UI quantity bridging (`UiQuantityLots`)

- `forecast_db.py`
  - forecast store DB access
  - system positions
  - TV signal log
  - reconciliation log
  - PnL tracking
  - latest webhook LTP storage

### 4.2 Angel browser automation

- `angel_web_order_bot.py`
  - Chrome attach/launch
  - Angel web login
  - OTP handling
  - watchlist search/add flow
  - order pad fill and submission
  - screenshots and HTML artifacts
  - hard timeouts and retries

- `angel_web_order_bot_selectors.json`
  - UI selectors used by the bot
  - if Angel DOM changes, this file is often the first thing to inspect

### 4.3 Configuration and startup

- `instrument_config.json`
  - enabled instruments
  - subsystem weights
  - system name alias map
  - broker routing
  - `QuantityMultiplier`
  - smart-chase flags
  - rollover config

- `start_unix_control_plane.sh`
  - Unix startup wrapper
  - environment variables for ngrok and remote Angel executor

- `start_windows_angel_worker.ps1`
  - Windows startup wrapper
  - environment variables for worker-only mode and Angel browser bot

### 4.4 Tests and harness

- `pytest.ini`
- `conftest.py`
- `test_server_order_handler.py`
- `test_angel_web_order_bot.py`
- `test_ncdex_integration.py`
- `test_server_start.py`
- `test_instrument_config.py`
- `test_direction_crosscheck.py`
- `test_smart_chase.py`

## 5. Current Endpoints

### 5.1 `/forecast`

Method:

- `POST`

Purpose:

- main TradingView forecast webhook

Expected JSON:

```json
{
  "SystemName": "S30A_GoldM",
  "Instrument": "GOLDM",
  "Netposition": 1,
  "ATR": 1200,
  "LTP": 72500.0,
  "Action": "buy"
}
```

Notes:

- `LTP` is optional
- `Action` is optional
- `Instrument` must match the top-level instrument key in `instrument_config.json`
- `SystemName` can be an alias if it maps through `system_name_map`

Behavior:

- returns immediately
- actual execution is asynchronous through worker threads

### 5.2 `/internal/angel-execute`

Method:

- `POST`

Purpose:

- internal Unix -> Windows remote Angel execution

Auth:

- `Authorization: Bearer <ANGEL_EXECUTOR_TOKEN>`

Expected JSON structure:

```json
{
  "request_id": "uuid-or-short-id",
  "source": "forecast_orchestrator",
  "order": {
    "User": "AABM826021",
    "Broker": "ANGEL",
    "Exchange": "NCDEX",
    "Tradingsymbol": "CASTOR20APR2026",
    "Symboltoken": "CASTOR20APR2026",
    "Tradetype": "buy",
    "Ordertype": "LIMIT",
    "Variety": "NORMAL",
    "Product": "CARRYFORWARD",
    "Validity": "DAY",
    "Quantity": 5,
    "UiQuantityLots": 1,
    "Price": "6525",
    "Netposition": 5,
    "UpdatedOrderRouting": "True",
    "ContractNameProvided": "True",
    "InstrumentType": "FUTCOM"
  }
}
```

### 5.3 `/status`

Method:

- `GET`

Returns:

- dry-run flag
- enabled instruments
- current forecasts
- current positions
- active overrides
- recent orders
- recent TradingView signals
- recent reconciliations

### 5.4 Legacy routes

- `/`
- `/forecast/angel`

These still exist for legacy webhook/order flows. The new design should prefer `/forecast` plus the orchestrator path.

## 6. Instrument Naming Rules

### 6.1 Payload instrument naming

The `Instrument` field in the webhook payload must use the instrument key from `instrument_config.json`, not the full exchange contract.

Examples:

- use `CASTOR`, not `CASTOR20APR2026`
- use `GUARGUM`, not `GUARGUM5` or `GUARGUM520APR2026`
- use `TURMERIC`, not `TMCFGRNZM`

### 6.2 NCDEX payload instrument names

The current NCDEX payload instruments are:

- `DHANIYA`
- `GUARSEED`
- `GUARGUM`
- `CASTOR`
- `COCUDAKL`
- `TURMERIC`
- `JEERA`

### 6.3 Subsystem alias mapping

Webhook `SystemName` values can be aliases. The orchestrator normalizes them to the configured subsystem key.

Example:

- raw `S45A_ZINC`
- normalized `S45A`

If a raw system name is not mapped for that instrument, the signal is logged but not used for trading.

## 7. Quantity Semantics

This is one of the most important design details.

### 7.1 Strategy target

The strategy target is expressed in strategy units, which for the Angel browser path must map to lots in the UI.

### 7.2 SmartAPI / internal expanded quantity

For Angel SmartAPI and some internal flows, quantity can be expressed using the legacy multiplier format:

- `1*5`
- `3*5`

This means:

- first number = lots
- second number = multiplier / lot size used in the API flow

`Validate_Quantity(...)` expands this to raw quantity, for example:

- `1*5 -> 5`
- `3*5 -> 15`

### 7.3 Browser quantity

The Angel browser UI quantity field for NCDEX is a `Lots` field. Therefore:

- if target is 1 lot, the UI must get `1`
- not `5`

This is why the order flow now preserves:

- `UiQuantityLots`

and uses that value when building the browser order payload.

### 7.4 Rule of thumb

- browser UI quantity = lots
- SmartAPI quantity = expanded / broker-facing quantity when applicable

## 8. Angel Browser UI Design

### 8.1 Browser mode

The browser worker is designed around a Chrome instance with remote debugging, usually:

- `127.0.0.1:9222`

The worker tries to:

1. attach to an existing Chrome debugger session
2. if not reachable, launch a dedicated Chrome instance

### 8.2 Angel web target page

The bot operates on:

- `https://www.angelone.in/trade/watchlist/chart`

### 8.3 Login model

The bot supports:

- existing already-logged-in browser session
- file-based login
- manual login fallback with timeout

Credentials and OTP files used by default on Windows:

- `angel_web_login_credentials.txt`
- `angel_web_login_otp.txt`

Optional fetcher script:

- `fetch_broker_email_otp.py`

### 8.4 Watchlist behavior

The default worker config uses:

- `ANGEL_WEB_WATCHLIST_INDEX=4`

The current behavior is:

1. select the configured watchlist tab
2. look for the target contract row
3. if missing, search and add the contract automatically
4. close search
5. reselect the watchlist
6. click `BUY` or `SELL`

### 8.5 Order pad behavior

For the order pad, the bot:

- sets product (`INT` vs `CF`) using button ids
- sets order type (`LIMIT` vs `MARKET`)
- fills quantity
- for limit orders, pauses before filling price
- types the limit price with per-digit delay
- pauses before submit
- pauses before confirm if confirm is present

### 8.6 Intentional pauses

Current visible pauses:

- pause before filling Angel limit price
- digit-by-digit typing delay for limit price
- pause before Angel submit click
- pause before Angel confirm click

These are logged explicitly.

### 8.7 Hard stop guardrails

The browser worker is designed not to spin indefinitely.

Current hard ceilings include:

- file login hard timeout: 180s
- watchlist add hard timeout: 45s
- order placement hard timeout: 150s
- manual login hard timeout: 300s
- max login identifier submit attempts: 3

## 9. Unix Startup and Commands

### 9.1 Main startup script

File:

- `start_unix_control_plane.sh`

Current placeholder values in the repo must be updated before use.

Template:

```bash
#!/usr/bin/env bash
set -euo pipefail

export NGROK_DOMAIN="<unix-ngrok-domain>"
export DISABLE_NGROK="false"
export ANGEL_EXECUTOR_ONLY="false"
export ANGEL_REMOTE_EXECUTION_URL="https://<windows-ngrok-domain>/internal/angel-execute"
export ANGEL_EXECUTOR_TOKEN="<shared-secret>"
export ANGEL_REMOTE_EXECUTION_TIMEOUT_SECONDS="180"

exec python3 Server_Start.py
```

Historically recovered values from logs:

- Unix public ngrok domain used: `auto2.in.ngrok.io`
- Windows worker ngrok domain used: `listen.ngrok.io`
- historical remote timeout before the later hardening: `90`

### 9.2 Run Unix control plane manually

```bash
cd /home/ubuntu/Work/OrderHandling
./start_unix_control_plane.sh
```

### 9.3 Example Unix cron line

```cron
50 8 * * * cronitor exec PIrGo1 screen -dmS unix_control_plane bash -lc 'cd /home/ubuntu/Work/OrderHandling && timeout 57600 ./start_unix_control_plane.sh >> /home/ubuntu/Work/OrderHandling/Start.log 2>&1'
```

### 9.4 Verify Unix is alive

```bash
curl -s http://127.0.0.1:5055/status
```

## 10. Windows Startup and Commands

### 10.1 Main startup script

File:

- `start_windows_angel_worker.ps1`

Purpose:

- starts Flask in worker-only mode
- enables ngrok
- configures Angel browser bot paths

Core environment variables:

- `NGROK_DOMAIN`
- `DISABLE_NGROK=false`
- `ANGEL_EXECUTOR_ONLY=true`
- `ANGEL_EXECUTOR_TOKEN`
- `ANGEL_EXECUTOR_PATH=/internal/angel-execute`
- `ANGEL_WEB_PROFILE_DIR`
- `ANGEL_WEB_DEBUGGER_ADDRESS`
- `ANGEL_WEB_CHROME_BINARY`
- `ANGEL_WEB_LOGIN_CREDENTIALS_PATH`
- `ANGEL_WEB_LOGIN_OTP_PATH`
- `ANGEL_WEB_INSTRUMENT_FILE`
- `ANGEL_WEB_ATTACH_ONLY=false`

### 10.2 Run Windows worker manually

```powershell
cd C:\Users\Administrator\Documents\Work\Code
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\Users\Administrator\Documents\Work\Code\start_windows_angel_worker.ps1"
```

If script execution is blocked in a current shell:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\start_windows_angel_worker.ps1
```

### 10.3 Task Scheduler / schtasks example

At startup:

```powershell
schtasks /Create /TN "Angel Windows Worker" /SC ONSTART /RU Administrator /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File \"C:\Users\Administrator\Documents\Work\Code\start_windows_angel_worker.ps1\""
```

### 10.4 Operational recommendation

The Windows worker should normally be:

- already running
- already logged in to Angel web before the trading session

This reduces remote timeout risk and avoids OTP delays during live orders.

## 11. Direct Angel Browser Bot Commands

All commands below are run from the repo root.

### 11.1 General help

```bash
./.venv/bin/python angel_web_order_bot.py --help
```

### 11.2 Launch a Chrome session for Angel

```bash
./.venv/bin/python angel_web_order_bot.py launch --debug-port 9222
```

### 11.3 Place an order from JSON

```bash
./.venv/bin/python angel_web_order_bot.py place --order-file angel_web_order_bot_order.example.json --watchlist-index 4 --submit-live --keep-open
```

### 11.4 Seed the Angel watchlist

```bash
./.venv/bin/python angel_web_order_bot.py seed-watchlist --instrument-file AngelInstrumentDetails.csv --watchlist-index 4 --keep-open
```

## 12. Payload Examples

### 12.1 Angel NCDEX example with Unix-side LTP fetch

Omit `LTP`:

```json
{
  "SystemName": "S30A",
  "Instrument": "CASTOR",
  "Netposition": 1,
  "ATR": 95,
  "Action": "BUY"
}
```

### 12.2 Angel NCDEX example with webhook-supplied LTP

```json
{
  "SystemName": "S30A",
  "Instrument": "CASTOR",
  "Netposition": 1,
  "ATR": 95,
  "LTP": 6524,
  "Action": "BUY"
}
```

### 12.3 Zerodha example

```json
{
  "SystemName": "S45A_ZINC",
  "Instrument": "ZINCMINI",
  "Netposition": 1,
  "ATR": 5.71,
  "Action": "BUY"
}
```

## 13. Test Commands

### 13.1 Full suite

```bash
./.venv/bin/python -m pytest -q
```

Latest recorded result on this workstream:

- `435 passed in 39.78s`

### 13.2 Focused suites used during this work

```bash
./.venv/bin/python -m pytest test_server_order_handler.py -q
./.venv/bin/python -m pytest test_angel_web_order_bot.py -q
./.venv/bin/python -m pytest test_ncdex_integration.py -q
./.venv/bin/python -m pytest test_server_start.py -q
./.venv/bin/python -m pytest test_instrument_config.py -q
```

## 14. Useful Runtime Commands

### 14.1 Inspect orchestrator status

```bash
curl -s http://127.0.0.1:5055/status
```

### 14.2 Send a test forecast webhook

```bash
curl -X POST http://127.0.0.1:5055/forecast \
  -H "Content-Type: application/json" \
  -d '{"SystemName":"S30A","Instrument":"CASTOR","Netposition":1,"ATR":95,"Action":"BUY"}'
```

### 14.3 Check the latest git state

```bash
git status -sb
git log --oneline -n 5
```

## 15. Database Commands

### 15.1 Delete all rows for CASTOR from the forecast DB

```bash
sqlite3 /home/ubuntu/Work/OrderHandling/forecast_store.db "
DELETE FROM subsystem_forecasts WHERE instrument='CASTOR';
DELETE FROM tradingview_signals WHERE instrument='CASTOR';
DELETE FROM system_positions WHERE instrument='CASTOR';
DELETE FROM order_log WHERE instrument='CASTOR';
DELETE FROM reconciliation_log WHERE instrument='CASTOR';
DELETE FROM realized_pnl WHERE instrument='CASTOR';
"
```

### 15.2 Verify CASTOR is cleared

```bash
sqlite3 /home/ubuntu/Work/OrderHandling/forecast_store.db "
SELECT 'subsystem_forecasts', COUNT(*) FROM subsystem_forecasts WHERE instrument='CASTOR'
UNION ALL
SELECT 'tradingview_signals', COUNT(*) FROM tradingview_signals WHERE instrument='CASTOR'
UNION ALL
SELECT 'system_positions', COUNT(*) FROM system_positions WHERE instrument='CASTOR'
UNION ALL
SELECT 'order_log', COUNT(*) FROM order_log WHERE instrument='CASTOR'
UNION ALL
SELECT 'reconciliation_log', COUNT(*) FROM reconciliation_log WHERE instrument='CASTOR'
UNION ALL
SELECT 'realized_pnl', COUNT(*) FROM realized_pnl WHERE instrument='CASTOR';
"
```

## 16. Angel Account and Credential Notes

Current live Angel user mapping in the code:

- `AABM826021` -> `Login_Credentials_Angel_Eshita.txt`

The older `E51339915` path was intentionally removed from active runtime use.

Important distinction:

- SmartAPI credentials file on Unix is not the same as the Angel web browser login file on Windows
- Unix SmartAPI and Windows browser login are separate authentication surfaces

## 17. Known Operational Failure Modes

### 17.1 `Invalid API Key or App not found`

Meaning:

- Unix SmartAPI login is using the wrong Angel app key or unapproved app/IP combination

Check:

- `Login_Credentials_Angel_Eshita.txt`
- app key
- client code
- password
- TOTP seed

### 17.2 `Unknown subsystem ... Signal logged but not used`

Meaning:

- the raw `SystemName` was not mapped to a valid subsystem for that instrument

Fix:

- correct the TradingView payload
- or add the alias to `system_name_map`

### 17.3 `OSError: [Errno 98] Address already in use`

Meaning:

- something is already listening on port `5055`

Fix:

- stop the old process before restarting the server

### 17.4 Watchlist row missing

The system now attempts to:

- search
- add the contract to the watchlist
- reselect the watchlist
- retry the order action

### 17.5 Browser quantity appears oversized

Check whether:

- browser order payload uses `quantity` as lots
- `UiQuantityLots` is present
- the UI quantity box is in `Lots`

### 17.6 Remote timeout waiting for Windows

This occurs when:

- Windows needs to relaunch Chrome
- login is required
- OTP fetch is slow

Hardening applied:

- Unix remote timeout increased to `180s`

### 17.7 Selenium stale element or connection reset

This can happen due to:

- Angel page rerender
- Chrome crash/restart
- debugger session reset

The current flow retries in bounded ways and fails instead of looping forever.

## 18. Design Decisions Added During This Workstream

- optional `LTP` support in `/forecast`
- Unix-side Angel LTP preflight when `LTP` is absent
- Windows worker-only execution path
- authenticated internal executor endpoint
- Eshita Angel credential mapping for `AABM826021`
- watchlist auto-add when the contract is missing
- explicit pause logging before price fill and submit/confirm clicks
- hard stop timeouts across Angel browser flows
- browser quantity uses lot semantics through `UiQuantityLots`
- test harness isolation improvements with `pytest.ini` and `conftest.py`

## 19. Files Changed During the Workstream

Primary files changed:

- `forecast_orchestrator.py`
- `Server_Order_Handler.py`
- `Server_Start.py`
- `forecast_db.py`
- `angel_web_order_bot.py`
- `instrument_config.json`
- `start_unix_control_plane.sh`
- `start_windows_angel_worker.ps1`
- `pytest.ini`
- `conftest.py`
- `test_angel_web_order_bot.py`
- `test_server_order_handler.py`
- `test_ncdex_integration.py`
- `test_server_start.py`
- `test_instrument_config.py`
- `test_direction_crosscheck.py`
- `test_smart_chase.py`

Reference documents created:

- `Kite_API_Change_Reference_2026-04-01.docx`
- `Kite_API_Operations_Runbook_2026-04-01.md`

## 20. Final Operational Summary

The intended steady-state setup is:

- Unix EC2 runs the main control plane
- Windows EC2 runs the Angel browser worker
- TradingView sends forecast payloads to Unix `/forecast`
- Unix computes and routes
- Zerodha executes locally
- Angel NCDEX uses Unix SmartAPI for price/contract work and Windows browser for execution
- `/status` on Unix is the main quick health endpoint
- the Angel browser should ideally stay running and logged in before trading hours

If this topology is maintained, the system is significantly more robust than the earlier single-host or ambiguous-quantity design.
