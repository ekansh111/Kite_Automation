#!/bin/bash
# ─────────────────────────────────────────────────────
# V2 Smoke Test — run before live trading
# Executes dry-run with overrides and checks for errors
#
# Usage:
#   ./smoke_test_v2.sh                    # test all strategies
#   ./smoke_test_v2.sh N_STD_4D_30SL_I    # test specific strategy
# ─────────────────────────────────────────────────────

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"
V2_SCRIPT="$SCRIPT_DIR/PlaceOptionsSystemsV2.py"
LOG_FILE="/tmp/v2_smoke_test_$(date +%Y%m%d_%H%M%S).log"

# Detect Python — prefer python3 (has kiteconnect), use python3.13 for pytest if available
PY=python3
if command -v python3.13 &>/dev/null; then
    PY_TEST=python3.13
else
    PY_TEST=python3
fi

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "═══════════════════════════════════════════════"
echo "  V2 Smoke Test — $(date '+%Y-%m-%d %H:%M:%S')"
echo "═══════════════════════════════════════════════"
echo ""

# Activate venv (optional — server may use system Python)
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
else
    echo -e "${YELLOW}INFO: No venv at $VENV_DIR — using system Python${NC}"
fi

# ─── Step 1: Unit Tests ──────────────────────────────
echo "Step 1: Running unit tests..."
if $PY_TEST -m pytest "$SCRIPT_DIR/test_PlaceOptionsSystemsV2.py" -v --tb=short 2>&1 | tee -a "$LOG_FILE"; then
    echo -e "${GREEN}PASS: All unit tests passed${NC}"
else
    echo -e "${RED}FAIL: Unit tests failed. Check output above.${NC}"
    exit 1
fi
echo ""

# ─── Step 2: Syntax Check ────────────────────────────
echo "Step 2: Syntax check..."
if $PY -c "import ast; ast.parse(open('$V2_SCRIPT').read()); print('OK')" 2>&1; then
    echo -e "${GREEN}PASS: Syntax OK${NC}"
else
    echo -e "${RED}FAIL: Syntax error in PlaceOptionsSystemsV2.py${NC}"
    exit 1
fi
echo ""

# ─── Step 3: Dry-Run ─────────────────────────────────
echo "Step 3: Running dry-run..."

# Build override args
if [ -n "$1" ]; then
    OVERRIDES="--override=$1"
    echo "  Strategy: $1"
else
    OVERRIDES="--override=N_STD_4D_30SL_I --override=N_STD_2D_55SL_I --override=SX_STD_4D_20SL_I --override=SX_STD_2D_100SL_I"
    echo "  Strategy: ALL (4 strategies)"
fi

echo "  Mode: DRY RUN"
echo ""

# Run and capture output (|| true prevents set -e from killing the script)
DRY_OUTPUT=$($PY "$V2_SCRIPT" --dry-run $OVERRIDES 2>&1) || true
DRY_EXIT=${PIPESTATUS[0]:-$?}

echo "$DRY_OUTPUT" | tee -a "$LOG_FILE"
echo ""

# ─── Step 4: Check for Errors ────────────────────────
echo "Step 4: Checking output for errors..."
ERRORS=0

# Check exit code
if [ $DRY_EXIT -ne 0 ]; then
    echo -e "${RED}  FAIL: Script exited with code $DRY_EXIT${NC}"
    ERRORS=$((ERRORS + 1))
fi

# Check for Python exceptions
if echo "$DRY_OUTPUT" | grep -qiE "Traceback|Exception|Error.*:"; then
    echo -e "${RED}  FAIL: Python exception detected:${NC}"
    echo "$DRY_OUTPUT" | grep -A2 -iE "Traceback|Exception|Error.*:" | head -10
    ERRORS=$((ERRORS + 1))
fi

# Check for repairRequired
if echo "$DRY_OUTPUT" | grep -q "repairRequired"; then
    echo -e "${YELLOW}  WARN: repairRequired state detected${NC}"
    echo "$DRY_OUTPUT" | grep "repairRequired"
fi

# Check for BLOCKED
if echo "$DRY_OUTPUT" | grep -q "BLOCKED"; then
    echo -e "${YELLOW}  WARN: Strategy entry blocked${NC}"
    echo "$DRY_OUTPUT" | grep "BLOCKED"
fi

# Check for ABORTED
if echo "$DRY_OUTPUT" | grep -q "ABORTED"; then
    echo -e "${RED}  FAIL: Exit aborted${NC}"
    echo "$DRY_OUTPUT" | grep "ABORTED"
    ERRORS=$((ERRORS + 1))
fi

# Check reconciliation ran
if echo "$DRY_OUTPUT" | grep -q "\[RECONCILE\]"; then
    echo -e "${GREEN}  OK: Reconciliation ran${NC}"
    echo "$DRY_OUTPUT" | grep "\[RECONCILE\]" | head -5
else
    echo -e "${YELLOW}  WARN: No reconciliation output found${NC}"
fi

echo ""

# ─── Step 5: Summary ─────────────────────────────────
echo "═══════════════════════════════════════════════"
if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}  SMOKE TEST PASSED — safe to go live${NC}"
    echo ""
    echo "  Run live with:"
    echo "  $PY $V2_SCRIPT $OVERRIDES"
else
    echo -e "${RED}  SMOKE TEST FAILED — $ERRORS error(s) found${NC}"
    echo "  Review log: $LOG_FILE"
fi
echo "═══════════════════════════════════════════════"
echo ""
echo "Log saved to: $LOG_FILE"
