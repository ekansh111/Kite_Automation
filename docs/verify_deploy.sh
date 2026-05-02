#!/bin/bash
# verify_deploy.sh — run on EC2 after uploading files via FileZilla.
# Verifies file integrity, config correctness, and runs the test suite.

set -e

# ─── EXPECTED CHECKSUMS (from local build, May 1 2026) ────────────
declare -A EXPECTED_SHA=(
    ["PlaceOptionsSystemsV2.py"]="f550abffdd1589ec94eef92e2d32bb18f1feff1e17f04b53c87e41394e984d24"
    ["itm_call_rollover.py"]="c3b983a20d3cd78c82630ba2a06b7e67978cddfa9a2b32770cc5fa721bf8b4b6"
    ["itm_call_daily_monitor.py"]="2eeac91bdb4c2fadf59c5893d56f4f94b98e1ec24dacf6a20a83f7822363d332"
    ["test_itm_call_dynamic_k.py"]="8b6b7b826eb283349b76896ca889c0ed67176ebccb8b46c3b160afcaae8eee6e"
    ["test_itm_call_integration.py"]="cf1e9bdb3bc93faa227f2403c6ed587c0e3b0a4f1dd00d07eb57e67cad9b1913"
    ["instrument_config.json"]="50f875b9f40cb8bb4f4d7520baec95109ecbdbd4b25d9f29e76ac118477fa844"
)

# Adjust this if your venv is elsewhere
PYTHON="${PYTHON:-./venv/bin/python}"
[ ! -x "$PYTHON" ] && PYTHON="${PYTHON:-./.venv/bin/python}"
[ ! -x "$PYTHON" ] && PYTHON=python3

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok() { echo -e "${GREEN}✓${NC} $1"; }
fail() { echo -e "${RED}✗${NC} $1"; exit 1; }
warn() { echo -e "${YELLOW}⚠${NC} $1"; }

echo "════════════════════════════════════════════════════════════"
echo "ITM Call v2.0 — Deployment Verification"
echo "════════════════════════════════════════════════════════════"
echo ""

# ─── 1. CHECKSUM VERIFICATION ──────────────────────────────────────
echo "[1/4] File integrity (sha256):"
for f in "${!EXPECTED_SHA[@]}"; do
    if [ ! -f "$f" ]; then
        fail "MISSING: $f"
    fi
    actual=$(sha256sum "$f" | cut -d' ' -f1)
    expected="${EXPECTED_SHA[$f]}"
    if [ "$actual" = "$expected" ]; then
        ok "$f"
    else
        fail "$f checksum mismatch (likely upload corruption — try re-uploading in BINARY mode)"
    fi
done
echo ""

# ─── 2. PYTHON IMPORTS ─────────────────────────────────────────────
echo "[2/4] Python imports:"
$PYTHON -c "
from PlaceOptionsSystemsV2 import (
    computeDynamicK, resolveKLongSingle, getRegimeAddon,
    REGIME_ADDON_TABLE, K_FLOOR, K_CEILING,
)
from itm_call_rollover import (
    AllocateLotsBalanced, ComputePositionSizeITM,
    PrepareSizingForIndex, RunCoordinatedRollover,
    BuildRolloverEmailHtml, BuildCombinedPortfolioEmail,
    POOL_ROUNDUP_THRESHOLD,
)
from itm_call_daily_monitor import (
    BuildDailyMonitorEmail, BuildAutoTrimEmail,
)
print('All imports OK')
" || fail "Import test failed"
ok "All new functions importable"
echo ""

# ─── 3. CONFIG VERIFICATION ────────────────────────────────────────
echo "[3/4] Config verification:"
$PYTHON -c "
import json
c = json.load(open('instrument_config.json'))
for k in ['NIFTY_ITM_CALL', 'BANKNIFTY_ITM_CALL']:
    cfg = c['options_allocation'].get(k, {})
    assert cfg.get('useDynamicK') == True, f'{k}: useDynamicK should be True'
    assert cfg.get('max_premium_pct_of_capital') == 0.03, f'{k}: cap should be 0.03'
    rs = cfg.get('regimeSignal', {})
    assert rs.get('recent_window') == 20
    assert rs.get('baseline_window') == 100
    aw = cfg['vol_weights'].get('asset_weight')
    assert aw == 0.125, f'{k}: asset_weight should be 0.125, got {aw}'
    print(f'  {k}: OK (useDynamicK=True, cap=3%, regime=20/100d, asset_weight=0.125)')
" || fail "Config verification failed"
ok "Config correct"
echo ""

# ─── 4. TEST SUITE ─────────────────────────────────────────────────
echo "[4/4] Running test suite:"
$PYTHON -m pytest test_itm_call_dynamic_k.py test_itm_call_integration.py -q 2>&1 | tail -5
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    fail "Tests failed — investigate before activating in production"
fi
ok "All 36 tests pass"
echo ""

echo "════════════════════════════════════════════════════════════"
echo -e "${GREEN}DEPLOYMENT VERIFIED — Safe to activate${NC}"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "Next steps:"
echo "  1. Run dry-run:  $PYTHON itm_call_rollover.py --force --dry-run"
echo "  2. Add cron job: 10 15 * * 1-5 cd \$PWD && $PYTHON itm_call_daily_monitor.py >> logs/monitor.log 2>&1"
echo "  3. Wait for next monthly expiry day for first live rollover"
