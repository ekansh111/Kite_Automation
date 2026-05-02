/**
 * Build comprehensive User Guide for ITM Call Dynamic K + Pooled Allocation framework.
 */
const fs = require('fs');
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  AlignmentType, LevelFormat, HeadingLevel, BorderStyle, WidthType,
  ShadingType, PageBreak, PageNumber, Header, Footer,
  TableOfContents,
} = require('docx');

const NAVY = '003366';
const ACCENT = '2E75B6';
const GREEN = '27AE60';
const RED = 'E74C3C';
const AMBER = 'F39C12';
const GREY_BG = 'F8F9FA';
const BORDER_COL = 'CCCCCC';
const TABLE_HEADER_BG = 'D5E8F0';
const CALLOUT_GREEN_BG = 'E8F5E9';

const cellBorder = { style: BorderStyle.SINGLE, size: 1, color: BORDER_COL };
const cellBorders = { top: cellBorder, bottom: cellBorder, left: cellBorder, right: cellBorder };

function H1(text) { return new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun(text)] }); }
function H2(text) { return new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(text)] }); }
function H3(text) { return new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun(text)] }); }
function P(text, opts = {}) {
  return new Paragraph({ spacing: { after: 120 }, children: [new TextRun({ text, ...opts })] });
}
function Bullet(text, opts = {}) {
  return new Paragraph({
    numbering: { reference: 'bullets', level: 0 },
    children: [new TextRun({ text, ...opts })],
  });
}
function Numbered(text) {
  return new Paragraph({
    numbering: { reference: 'numbers', level: 0 },
    children: [new TextRun(text)],
  });
}
function CodeBlock(lines) {
  return lines.map(line => new Paragraph({
    children: [new TextRun({ text: line, font: 'Courier New', size: 20 })],
    shading: { fill: GREY_BG, type: ShadingType.CLEAR },
  }));
}
function Callout(text, color = GREEN) {
  return new Paragraph({
    spacing: { before: 120, after: 120 },
    shading: { fill: CALLOUT_GREEN_BG, type: ShadingType.CLEAR },
    border: {
      top: { style: BorderStyle.SINGLE, size: 12, color, space: 6 },
      bottom: { style: BorderStyle.SINGLE, size: 12, color, space: 6 },
    },
    children: [new TextRun({ text, bold: true, color: NAVY })],
  });
}
function Spacer() { return new Paragraph({ children: [new TextRun('')] }); }
function PageBreakP() { return new Paragraph({ children: [new PageBreak()] }); }

function makeTable(rows, columnWidths) {
  const totalWidth = columnWidths.reduce((a, b) => a + b, 0);
  const tableRows = rows.map((cells, rowIdx) => {
    const isHeader = rowIdx === 0;
    return new TableRow({
      children: cells.map((cellText, colIdx) => new TableCell({
        borders: cellBorders,
        width: { size: columnWidths[colIdx], type: WidthType.DXA },
        shading: { fill: isHeader ? TABLE_HEADER_BG : 'FFFFFF', type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 120, right: 120 },
        children: [new Paragraph({
          children: [new TextRun({
            text: String(cellText), bold: isHeader, size: 20,
            color: isHeader ? NAVY : '333333',
          })],
        })],
      })),
    });
  });
  return new Table({ width: { size: totalWidth, type: WidthType.DXA }, columnWidths, rows: tableRows });
}

const children = [];

// COVER
children.push(
  new Paragraph({
    alignment: AlignmentType.CENTER, spacing: { before: 3000, after: 240 },
    children: [new TextRun({ text: 'ITM Call Dynamic K + Pooled Allocation',
      bold: true, size: 44, color: NAVY })],
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER, spacing: { after: 480 },
    children: [new TextRun({ text: 'User Guide', bold: true, size: 36, color: ACCENT })],
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER, spacing: { after: 240 },
    children: [new TextRun({ text: 'Version 2.0', size: 24, color: '555555' })],
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER, spacing: { after: 240 },
    children: [new TextRun({ text: 'Date: May 1, 2026', size: 24, color: '555555' })],
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER, spacing: { before: 2400, after: 120 },
    children: [new TextRun({
      text: 'A complete reference for operating, configuring, and monitoring the long ITM call buying system.',
      italics: true, size: 22, color: '888888',
    })],
  }),
  PageBreakP(),
);

children.push(H1('Table of Contents'));
children.push(new TableOfContents('Contents', { hyperlink: true, headingStyleRange: '1-3' }));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 1. WHAT THIS IS
// ════════════════════════════════════════════════════════════════
children.push(H1('1. Overview'));

children.push(H2('1.1 What this system does'));
children.push(P(
  'This system buys 4 to 5 percent in-the-money (ITM) monthly call options on NIFTY and BANKNIFTY ' +
  'and rolls them at expiry to the next month. It treats long calls as a leveraged equity exposure ' +
  '(part of the Equity bucket in the capital allocation framework), not as a volatility play.'
));

children.push(P('The system runs in two parts:'));
children.push(Numbered('Monthly rollover (itm_call_rollover.py): runs on monthly expiry day at 3:00 PM, exits the current month\'s ITM call, and buys the next month\'s ITM call in a 2-leg execution.'));
children.push(Numbered('Daily monitor (itm_call_daily_monitor.py): runs daily ~3:00 PM, computes drift vs entry conditions, and emails alerts on threshold breaches. No automatic rebalancing.'));

children.push(H2('1.2 What changed in v2.0'));
children.push(P('The previous version used a static K=0.18 to size positions. v2.0 introduces:'));
children.push(Bullet('Greeks-based dynamic K computed from the option\'s actual delta, gamma, vega, and theta at entry.'));
children.push(Bullet('Three K scenarios (kBase, kStressMove, kVegaCrush) — sizing uses the worst case (max of the three).'));
children.push(Bullet('Adaptive IV shock: base shock by DTE, plus VIX-bucket addon, plus 100-day realized-vol regime addon.'));
children.push(Bullet('Premium-at-risk cap: 3 percent of capital per index as a hard outlay ceiling.'));
children.push(Bullet('Pooled allocation: NIFTY and BANKNIFTY share the combined daily vol budget; lots are allocated jointly with a balance-preserving rule.'));
children.push(Bullet('Comprehensive emails showing K computation, IV shock construction, pooled allocation iterations, and a once-per-cycle combined portfolio summary.'));
children.push(Bullet('Daily drift monitoring with alert emails (NO auto-rebalancing — strategy character preserved).'));

children.push(H2('1.3 Activation flag'));
children.push(P(
  'The new framework is activated by setting useDynamicK: true in instrument_config.json (per index). ' +
  'When false, the legacy static K=0.18 path runs unchanged. Currently set to true for both NIFTY_ITM_CALL ' +
  'and BANKNIFTY_ITM_CALL.'
));

children.push(H2('1.4 Files in this system'));
children.push(makeTable([
  ['File', 'Purpose'],
  ['itm_call_rollover.py', 'Main entry script. Runs on monthly expiry day.'],
  ['itm_call_daily_monitor.py', 'Daily drift monitor. Run via cron ~3:00 PM.'],
  ['PlaceOptionsSystemsV2.py', 'Greeks, IV solver, dynamic K computation.'],
  ['instrument_config.json', 'Per-index config: weights, useDynamicK, premium cap, regime.'],
  ['itm_call_state.json', 'Persistent state per index (status, lots, entry).'],
], [3500, 5860]));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 2. CONCEPTUAL FRAMEWORK
// ════════════════════════════════════════════════════════════════
children.push(H1('2. Conceptual Framework'));

children.push(H2('2.1 Capital allocation'));
children.push(P('The system inherits its capital from the broader allocation framework:'));
children.push(makeTable([
  ['Layer', 'Value', 'Purpose'],
  ['Base capital', 'Rs 99,99,999', 'Total trading capital'],
  ['Annual vol target', '50%', 'Maximum annual P&L volatility'],
  ['Sector weight (Equities)', '0.3', 'Fraction allocated to equity strategies'],
  ['Asset weight (per ITM call index)', '0.125', 'Each of NIFTY/BANK gets half of the combined call bucket'],
  ['Asset DM (diversification multiplier)', '3.9', 'Boost from non-correlation with other assets'],
  ['Combined product', '0.14625', 'Sector × Asset × DM'],
  ['Daily vol budget per index', 'Rs 45,703', 'capital × annual_pct × product / sqrt(256) per index'],
  ['Pooled budget (combined)', 'Rs 91,406', 'Sum of per-index budgets, shared across indices'],
  ['Premium cap per index', 'Rs 3,00,000', '3% of capital — max outlay any single index can have'],
], [3700, 2200, 3460]));

children.push(H2('2.2 K — what it represents'));
children.push(P(
  'K is the ratio of expected daily P&L volatility to the option premium. It answers: "what fraction of ' +
  'premium does the position move per day in a stress scenario?" Higher K means higher daily P&L volatility ' +
  'per rupee of premium, so fewer lots can fit within the daily vol budget.'
));
children.push(P('Formula:'));
children.push(...CodeBlock([
  'dailyVolPerLot = K × premium × lotSize',
  'lots_vol = floor(daily_vol_budget / dailyVolPerLot)',
]));

children.push(H2('2.3 The three K scenarios'));
children.push(P(
  'For long premium positions, sizing uses the worst (max) of three independent stress scenarios.'
));
children.push(makeTable([
  ['Scenario', 'Spot move', 'IV change', 'What it tests'],
  ['kBase', '-1 sigma', '0', 'Normal daily vol target'],
  ['kStressMove', '-1.5 sigma', '0', 'Fat-tail spot move with no IV cushion'],
  ['kVegaCrush', '-1 sigma', '-shock', 'Adverse spot + IV crush (event vol collapse)'],
], [2500, 2200, 2000, 2660]));
children.push(P('K_use = max(kBase, kStressMove, kVegaCrush), then clamped to [0.20, 5.00].'));

children.push(P(
  'For long calls, all stresses use NEGATIVE spot moves and (for kVegaCrush) NEGATIVE IV changes. ' +
  'This is intentional: a long call loses when spot drops (delta loss) and when IV crushes (vega loss). ' +
  'The corresponding P&L magnitudes are the loss the position can incur in that scenario.'
));

children.push(H2('2.4 IV shock construction'));
children.push(P('The shock that feeds kVegaCrush is built additively:'));
children.push(...CodeBlock([
  'shock = baseShock(DTE) + vixAddon(VIX) + regimeAddon(realized_vol_20d / realized_vol_100d)',
  'shock = max(0, min(shock, 30vp))   # floor at 0, cap at 30 vp',
]));

children.push(H3('2.4.1 Base shock by DTE'));
children.push(P('Front-end IV is more fragile than back-end. The base shock scales inversely with DTE:'));
children.push(makeTable([
  ['DTE bucket', 'Base shock'],
  ['0 (expiry day)', '18 vp'],
  ['1', '15 vp'],
  ['2', '12 vp'],
  ['3-5', '10 vp'],
  ['6-10', '8 vp'],
  ['11-21', '6 vp'],
  ['22-45 (monthly)', '4 vp'],
], [4500, 4860]));

children.push(H3('2.4.2 VIX addon'));
children.push(P('When VIX is elevated, IV moves tend to be larger. The addon scales with VIX level:'));
children.push(makeTable([
  ['VIX range', 'Addon'],
  ['<14 (calm)', '0 vp'],
  ['14-18 (normal)', '+2 vp'],
  ['18-24 (elevated)', '+4 vp'],
  ['24-30 (stressed)', '+6 vp'],
  ['30+ (panic)', '+8 vp'],
], [4500, 4860]));

children.push(H3('2.4.3 Regime addon (realized vol ratio)'));
children.push(P(
  'For long monthly calls, the system compares 20-day realized vol to 100-day baseline. This catches ' +
  'regime shifts where recent vol differs from the longer-run average. Self-calibrates per instrument.'
));
children.push(makeTable([
  ['Ratio (recent / baseline)', 'Addon', 'Meaning'],
  ['< 0.70', '-2 vp', 'Strongly contracting regime'],
  ['0.70-0.90', '-1 vp', 'Mildly contracting'],
  ['0.90-1.10', '0 vp', 'Near baseline (normal)'],
  ['1.10-1.30', '+2 vp', 'Mildly expanding'],
  ['1.30-1.60', '+4 vp', 'Significantly expanding'],
  ['1.60-2.00', '+6 vp', 'Strongly expanding'],
  ['>= 2.00', '+8 vp', 'Extreme regime shift'],
], [3500, 1500, 4360]));

children.push(H2('2.5 Premium-at-risk cap'));
children.push(P(
  'Each index has a hard outlay ceiling of 3 percent of capital (Rs 3,00,000). If vol-target sizing wants ' +
  'more lots than the cap allows, the cap binds. This protects against catastrophic loss in any single ' +
  'cycle — even if the vol math wants a large position when premiums look cheap.'
));
children.push(...CodeBlock([
  'lots_vol = floor(daily_vol_budget / (K × premium × lotSize))',
  'lots_cap = floor(max_premium_outlay / (premium × lotSize))',
  'finalLots = max(1, min(lots_vol, lots_cap))',
]));

children.push(H2('2.6 Pooled allocation (balance-preserving)'));
children.push(P(
  'NIFTY and BANKNIFTY share the combined daily vol budget. After per-index floors are computed, the ' +
  'leftover pool is allocated iteratively using a balance-preserving rule with NIFTY as tiebreaker.'
));
children.push(P('Algorithm:'));
children.push(Numbered('Floor each index independently using its per-index budget (min 1 lot each).'));
children.push(Numbered('Pool the leftover budget across both indices.'));
children.push(Numbered('Iteratively add lots: identify primary = index with FEWER current lots (tie -> NIFTY).'));
children.push(Numbered('Try to add 1 lot to primary if leftover >= 80% of primary.dvpl AND new outlay <= cap.'));
children.push(Numbered('If primary doesn\'t fit, fall back to secondary index for utilization.'));
children.push(Numbered('Stop when neither index can fit another lot.'));

children.push(H3('2.6.1 The 80% round-up rule'));
children.push(P(
  'When pooled leftover lands in the 80-100% zone of an index\'s dvpl, the algorithm adds the lot anyway ' +
  '— accepting up to 20 percent over-budget per lot. This avoids wasting capacity that\'s "almost enough" ' +
  'for another lot.'
));

children.push(H3('2.6.2 NIFTY tiebreaker'));
children.push(P(
  'When both indices have the same lot count after floor allocation, NIFTY gets the next extra lot. NIFTY ' +
  'is more liquid, has lower vega per lot, and lower transaction costs — so it\'s the safer addition when ' +
  'both options are equal.'
));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 3. CONFIGURATION REFERENCE
// ════════════════════════════════════════════════════════════════
children.push(H1('3. Configuration Reference'));

children.push(H2('3.1 instrument_config.json structure'));
children.push(P('The relevant block for ITM call buying:'));
children.push(...CodeBlock([
  '{',
  '  "account": {',
  '    "base_capital": 9999999,',
  '    "annual_vol_target_pct": 0.50',
  '  },',
  '  "options_allocation": {',
  '    "NIFTY_ITM_CALL": {',
  '      "vol_weights": {',
  '        "sector_weight": 0.3,',
  '        "asset_weight": 0.125,',
  '        "asset_DM": 3.9',
  '      },',
  '      "useDynamicK": true,',
  '      "max_premium_pct_of_capital": 0.03,',
  '      "regimeSignal": {',
  '        "metric": "realized_vol",',
  '        "recent_window": 20,',
  '        "baseline_window": 100',
  '      }',
  '    },',
  '    "BANKNIFTY_ITM_CALL": {',
  '      "...": "same structure"',
  '    }',
  '  }',
  '}',
]));

children.push(H2('3.2 Field reference'));
children.push(makeTable([
  ['Field', 'Type', 'Default', 'Purpose'],
  ['vol_weights.sector_weight', 'float', '0.3', 'Equity bucket weight'],
  ['vol_weights.asset_weight', 'float', '0.125', 'Per-index allocation within bucket'],
  ['vol_weights.asset_DM', 'float', '3.9', 'Diversification multiplier'],
  ['useDynamicK', 'bool', 'true', 'Activate dynamic K + pooled allocation'],
  ['max_premium_pct_of_capital', 'float', '0.03', 'Max outlay per index as fraction of capital'],
  ['regimeSignal.metric', 'string', 'realized_vol', 'Comparison metric (only realized_vol supported now)'],
  ['regimeSignal.recent_window', 'int', '20', 'Days for recent vol (matches monthly hold)'],
  ['regimeSignal.baseline_window', 'int', '100', 'Days for baseline vol (long-run average)'],
], [3500, 1200, 1500, 3160]));

children.push(H2('3.3 ITM_CONFIG (in itm_call_rollover.py)'));
children.push(P('Per-index trading parameters (not user-editable in JSON, but shown for reference):'));
children.push(makeTable([
  ['Field', 'NIFTY', 'BANKNIFTY'],
  ['underlying_ltp_key', 'NSE:NIFTY 50', 'NSE:NIFTY BANK'],
  ['exchange', 'NFO', 'NFO'],
  ['strike_step', '50', '100'],
  ['itm_pct_min', '4.0%', '4.0%'],
  ['itm_pct_max', '5.0%', '5.0%'],
  ['exec_config_key', 'NIFTY_OPT', 'BANKNIFTY_OPT'],
  ['alloc_key', 'NIFTY_ITM_CALL', 'BANKNIFTY_ITM_CALL'],
], [3500, 2900, 2960]));

children.push(H2('3.4 Constants (PlaceOptionsSystemsV2.py)'));
children.push(makeTable([
  ['Constant', 'Value', 'Meaning'],
  ['K_FLOOR', '0.20', 'Minimum K value (clamps tiny K from low Greeks)'],
  ['K_CEILING', '5.00', 'Maximum K value (data quality guard)'],
  ['IV_SHOCK_CAP_VP', '30', 'Maximum total IV shock (vol points)'],
  ['STRESS_MOVE_MULTIPLIER', '1.5', 'kStressMove uses 1.5 × expected move'],
  ['BID_ASK_SPREAD_GATE', '0.30', 'Reject if spread > 30% of mid (quote quality)'],
  ['QUOTE_STALE_SECONDS', '60', 'Reject quote older than 60s during market hours'],
  ['POOL_ROUNDUP_THRESHOLD', '0.80', 'Pool round-up: buy lot if leftover >= 80% of dvpl'],
], [3500, 1500, 4360]));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 4. DAILY OPERATIONS
// ════════════════════════════════════════════════════════════════
children.push(H1('4. Daily Operations'));

children.push(H2('4.1 What runs daily'));
children.push(P('Two scheduled tasks:'));
children.push(makeTable([
  ['Script', 'When', 'Purpose'],
  ['itm_call_rollover.py', 'Last Tuesday of month, 3:00 PM IST', 'Execute monthly rollover'],
  ['itm_call_daily_monitor.py', 'Every trading day, ~3:00 PM IST', 'Compute drift, send alerts'],
], [3500, 3000, 2860]));

children.push(H2('4.2 Manual invocations'));
children.push(P('Common command-line usage:'));
children.push(...CodeBlock([
  '# Print current state of both indices',
  'python itm_call_rollover.py --status',
  '',
  '# Dry-run on a non-expiry day (logs decisions, no orders)',
  'python itm_call_rollover.py --force --dry-run',
  '',
  '# Force a real rollover even if not expiry day (use with caution)',
  'python itm_call_rollover.py --force',
  '',
  '# Cold start: first time entering, skip the EXIT leg',
  'python itm_call_rollover.py --first-run',
  '',
  '# Run for a single index only',
  'python itm_call_rollover.py --index=NIFTY',
  '',
  '# Daily monitor',
  'python itm_call_daily_monitor.py',
  '',
  '# Daily monitor in dry-run (no emails)',
  'python itm_call_daily_monitor.py --dry-run',
]));

children.push(H2('4.3 Cron setup (recommended)'));
children.push(P('Add these lines to your crontab:'));
children.push(...CodeBlock([
  '# Monthly rollover — runs every weekday at 15:00 IST',
  '#   (script internally checks if today is monthly expiry; exits if not)',
  '0 15 * * 1-5 cd /path/to/Kite_API && .venv/bin/python itm_call_rollover.py >> logs/rollover.log 2>&1',
  '',
  '# Daily drift monitor — runs every weekday at 15:10 IST',
  '10 15 * * 1-5 cd /path/to/Kite_API && .venv/bin/python itm_call_daily_monitor.py >> logs/monitor.log 2>&1',
]));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 5. MONTHLY ROLLOVER WORKFLOW
// ════════════════════════════════════════════════════════════════
children.push(H1('5. Monthly Rollover Workflow'));

children.push(H2('5.1 What happens on expiry day'));
children.push(P('When itm_call_rollover.py runs on monthly expiry day at 3:00 PM, it:'));
children.push(Numbered('Checks if today is monthly expiry (skips if not, unless --force).'));
children.push(Numbered('Loads vol budgets for each index based on current effective capital.'));
children.push(Numbered('For each index, prepares sizing inputs (Phase 1): fetches spot, finds expiries, selects ITM strike, resolves dynamic K, computes initial sizing.'));
children.push(Numbered('Runs pooled allocation (Phase 2): calls AllocateLotsBalanced across all eligible indices.'));
children.push(Numbered('For each index, executes orders (Phase 3): exits the current month\'s position (LEG 1), buys the next month\'s position (LEG 2) using SmartChase execution.'));
children.push(Numbered('Sends per-index rollover email + a combined portfolio email.'));

children.push(H2('5.2 Decision flow diagram'));
children.push(...CodeBlock([
  'main()',
  '  |',
  '  +-- For each index: check expiry, recover state',
  '  |',
  '  +-- Decide flow:',
  '  |     useDynamicK true => RunCoordinatedRollover',
  '  |     useDynamicK false => Existing per-index ExecuteRollover loop',
  '  |',
  '  +-- RunCoordinatedRollover (new path):',
  '  |     |',
  '  |     +-- Phase 1: PrepareSizingForIndex(NIFTY)   -> sizing inputs',
  '  |     |              PrepareSizingForIndex(BANK)  -> sizing inputs',
  '  |     |',
  '  |     +-- Phase 2: AllocateLotsBalanced({NIFTY, BANK}) -> {N: lots, B: lots}',
  '  |     |',
  '  |     +-- Phase 3: ExecuteRollover(NIFTY, OverrideFinalLots=N, KMeta=...) -> result',
  '  |                  ExecuteRollover(BANK,  OverrideFinalLots=B, KMeta=...) -> result',
  '  |',
  '  +-- Send per-index rollover emails',
  '  +-- Send combined portfolio email',
]));

children.push(H2('5.3 What you receive in email'));
children.push(P('After a successful rollover, you get TWO types of emails:'));
children.push(H3('5.3.1 Per-index rollover email'));
children.push(P('One email per index. Sections:'));
children.push(Bullet('Header: navy banner with index name and date.'));
children.push(Bullet('Status banner: green for SUCCESS, red for FAILED.'));
children.push(Bullet('Contract & Market Data: spot, strike, premium, BS theoretical.'));
children.push(Bullet('Position Sizing Formula: step-by-step lot computation.'));
children.push(Bullet('Dynamic K Computation: solved IV, Greeks, all 3 K scenarios with binding row highlighted, K_use callout. (NEW in v2.0)'));
children.push(Bullet('IV Shock Construction: base + VIX + regime additive breakdown. (NEW in v2.0)'));
children.push(Bullet('Pooled Allocation: floor allocation, pool leftover, iterations table, final allocation. (NEW in v2.0)'));
children.push(Bullet('Strike Selection Candidates: all 4-5% ITM strikes considered + which were rejected.'));
children.push(Bullet('Price Validation: BS theo vs market price for selected strike.'));
children.push(Bullet('Leg 2 Entry: order details, fill price, slippage.'));
children.push(Bullet('Roll Summary: P&L overview from old to new contract.'));

children.push(H3('5.3.2 Combined portfolio email'));
children.push(P('One email per cycle (after both indices complete). Sections:'));
children.push(Bullet('Per-Index Breakdown table: lots, qty, premium, outlay, worst-day MTM for each index, plus a COMBINED row.'));
children.push(Bullet('Capital Usage callout: combined outlay percent of capital, pooled vol budget utilization, leftover, worst-day MTM, max loss at expiry.'));

children.push(H2('5.4 Sample today output (live data)'));
children.push(P('Output from a dry-run on May 1, 2026 with useDynamicK=true:'));
children.push(...CodeBlock([
  '[NIFTY-LONG][DYNAMIC-K] IV shock: base=4vp + VIX=4vp (VIX=18.46) + regime=+0vp',
  '                      (ratio=1.0763) = 8.0vp',
  '[NIFTY-LONG][DYNAMIC-K] kForSizing=0.2480 (raw=0.2480, binding=kVegaCrush)',
  '                      kBase=0.1294 kStressMove=0.1899 kVegaCrush=0.2480',
  '                      IV=16.06% expMove=242.75 ivShock=8.0vp',
  '',
  '[NIFTY] Sizing: lots=1 qty=65 premium=1592.90 K=0.248',
  '        dailyVol/lot=25,678 budget=45,703 cap=Rs 300,000',
  '',
  '[POOL] Pooled allocation result: {NIFTY: 1}',
  '[POOL] Vol used: Rs 25,678 / 91,406 (28.1%), leftover Rs 65,728',
]));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 6. DAILY MONITORING
// ════════════════════════════════════════════════════════════════
children.push(H1('6. Daily Monitoring'));

children.push(H2('6.1 What it does'));
children.push(P(
  'The daily monitor (itm_call_daily_monitor.py) computes drift between today and entry conditions. It ' +
  'sends alerts when thresholds are breached but does NOT auto-rebalance. The strategy is hold-to-expiry; ' +
  'the monitor exists for visibility, not intervention.'
));

children.push(H2('6.2 Drift triggers'));
children.push(makeTable([
  ['Metric', 'Threshold', 'Action'],
  ['Spot drift from entry', 'plus or minus 5%', 'Alert email'],
  ['VIX drift from entry', 'plus or minus 40%', 'Alert email'],
  ['K_use rises 2x or more vs entry', '2x', 'Alert email'],
  ['Capital drift', 'plus or minus 15%', 'Alert email'],
  ['DTE crosses 14', 'DTE=14', 'Alert email (consider early roll)'],
  ['Outlay > 4% of capital', '4%', 'AUTO-TRIM — but not yet implemented; emits alert'],
], [3500, 2200, 3660]));

children.push(H2('6.3 Daily monitor email'));
children.push(P('When at least one threshold is breached, you get an alert email with:'));
children.push(Bullet('Amber status banner (vs green when all metrics OK).'));
children.push(Bullet('Drift Metrics table: each metric with entry value, today value, drift percent, alert flag.'));
children.push(Bullet('Position Status panel: symbol, lots, qty, MTM percent, current outlay percent of capital.'));
children.push(Bullet('Recommended Review section: human-readable suggestions (hold / roll / trim).'));

children.push(H2('6.4 Auto-trim status'));
children.push(Callout(
  'IMPORTANT: Auto-trim execution on cap-breach is intentionally NOT yet implemented. The monitor ' +
  'only emits a CRITICAL alert email when outlay exceeds 4% of capital. Manual review and execution ' +
  'is required until the auto-trim function has been validated through extended dry-run cycles.',
  AMBER,
));

children.push(H2('6.5 Entry snapshot requirement'));
children.push(P(
  'For drift comparison to work properly, the rollover script needs to save entry-time snapshot data ' +
  '(entry_spot, entry_vix, entry_k, entry_capital, entry_dte) into itm_call_state.json. This was ' +
  'flagged as a follow-up. Until added, the daily monitor will show "?" for entry values it cannot find.'
));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 7. EMAIL NOTIFICATIONS REFERENCE
// ════════════════════════════════════════════════════════════════
children.push(H1('7. Email Notifications Reference'));

children.push(H2('7.1 All email types'));
children.push(makeTable([
  ['Email', 'Trigger', 'Frequency'],
  ['Per-index rollover email', 'After each index entry', 'Once per index per cycle'],
  ['Combined portfolio email', 'After both indices complete', 'Once per cycle'],
  ['Daily monitor alert', 'Drift threshold breached', 'On trigger only'],
  ['Auto-trim notification', 'Cap-breach trim executed', 'On trigger only (not yet wired)'],
  ['Critical failure email', 'LEG 2 fails after LEG 1', 'On failure only'],
], [3500, 3000, 2860]));

children.push(H2('7.2 Visual hierarchy'));
children.push(P('All emails use a consistent palette:'));
children.push(makeTable([
  ['Color', 'Hex', 'Usage'],
  ['Navy', '#003366', 'Header bar, table headers, callout borders'],
  ['Accent Blue', '#2E75B6', 'Section underlines, binding row highlights'],
  ['Green', '#27AE60', 'SUCCESS banner, K_use callout border'],
  ['Red', '#E74C3C', 'FAILED banner, negative P&L, drift alerts'],
  ['Amber', '#F39C12', 'Daily monitor drift alert banner'],
  ['Grey BG', '#F8F9FA', 'Alternating table rows, info panels'],
], [2000, 1500, 5860]));

children.push(H2('7.3 Email recipient configuration'));
children.push(P('Edit the following constants in itm_call_rollover.py and itm_call_daily_monitor.py:'));
children.push(...CodeBlock([
  'EMAIL_FROM = "ekansh.n111@gmail.com"',
  'EMAIL_FROM_PASSWORD = "<gmail-app-password>"',
  'EMAIL_TO = "ekansh.n@gmail.com"',
  'EMAIL_SMTP = "smtp.gmail.com"',
  'EMAIL_PORT = 465',
]));
children.push(P('Email failures are logged but do NOT block trading.'));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 8. TROUBLESHOOTING
// ════════════════════════════════════════════════════════════════
children.push(H1('8. Troubleshooting Guide'));

children.push(H2('8.1 Common issues'));
children.push(makeTable([
  ['Symptom', 'Cause', 'Fix'],
  ['Token error on first run of day', 'Kite access token expired (daily refresh needed)', 'Run python Auto3_Fetch_Accesstoken.py first'],
  ['ALL strikes rejected (BS_DEVIATION/INTRINSIC_OVERPAY)', 'Stale quotes (market closed or holiday)', 'Wait for market open; system correctly refuses stale data'],
  ['Coordinated rollover skipped, fell back to per-index', 'useDynamicK is false in config', 'Set useDynamicK to true in instrument_config.json'],
  ['No vol budget configured for index', 'Missing options_allocation entry in config', 'Verify NIFTY_ITM_CALL / BANKNIFTY_ITM_CALL exists'],
  ['Email not received', 'SMTP credentials wrong or Gmail app password expired', 'Check EMAIL_FROM_PASSWORD; regenerate Gmail app password'],
  ['K resolution falls back to static K=0.18 silently', 'Quote spread too wide, IV near bounds, or stale data', 'Check log for [DYNAMIC-K] fallback reason'],
  ['Floor lots = 1 even when K is reasonable', 'BANKNIFTY: dvpl > per-index budget. Pool will recover.', 'This is normal — pool allocation recovers it'],
  ['Pool leftover stays high', 'Both indices at floor + remaining < 80% threshold', 'Working as designed — protects against over-budget'],
  ['Coordinated runner returns empty', 'All indices failed PrepareSizingForIndex', 'Check logs for why each index failed (usually quote quality)'],
], [3500, 2900, 2960]));

children.push(H2('8.2 Debug commands'));
children.push(...CodeBlock([
  '# Test imports without running anything',
  'python -c "from itm_call_rollover import RunCoordinatedRollover, AllocateLotsBalanced; print(\'OK\')"',
  '',
  '# Verify config flag is set correctly',
  'python -c "import json; c=json.load(open(\'instrument_config.json\')); ' +
  'print(c[\'options_allocation\'][\'NIFTY_ITM_CALL\'][\'useDynamicK\'])"',
  '',
  '# Run unit tests',
  'pytest test_itm_call_dynamic_k.py -v',
  '',
  '# Run integration tests (no Kite needed)',
  'pytest test_itm_call_integration.py -v',
  '',
  '# Print current state',
  'python itm_call_rollover.py --status',
  '',
  '# Force a dry-run on any day',
  'python itm_call_rollover.py --force --dry-run',
]));

children.push(H2('8.3 Reading the log'));
children.push(P('Key log markers to look for:'));
children.push(makeTable([
  ['Log line prefix', 'Meaning'],
  ['[NIFTY-LONG][DYNAMIC-K]', 'Dynamic K resolution is running for NIFTY'],
  ['IV shock: base=Xvp + VIX=Xvp + regime=Xvp', 'IV shock construction breakdown'],
  ['kForSizing=X.XXXX (binding=Y)', 'Final K and which scenario binds'],
  ['[POOL] Pooled allocation result', 'Joint allocation completed'],
  ['LEG 1: SELL ...', 'Exit of old contract starting'],
  ['LEG 2: BUY ...', 'Entry of new contract starting'],
  ['[CRITICAL] LEG 2 failed', 'Position now flat — manual recovery required'],
  ['Falling back to static K', 'Dynamic K failed quality gate; using K_TABLE_SINGLE'],
], [4500, 4860]));

children.push(H2('8.4 Rolling back to static K'));
children.push(P(
  'If dynamic K behaves unexpectedly and you want to revert to the legacy static K=0.18 path, set ' +
  'useDynamicK to false in instrument_config.json for both indices. The pre-existing path runs unchanged.'
));
children.push(...CodeBlock([
  '"NIFTY_ITM_CALL": {',
  '  "vol_weights": {...},',
  '  "useDynamicK": false,    <-- revert to static K=0.18',
  '  ...',
  '}',
]));

children.push(H2('8.5 Crash recovery'));
children.push(P(
  'If LEG 2 fails after LEG 1 succeeds (rare but possible), the position is FLAT but the database shows ' +
  'an incomplete rollover. On next run:'
));
children.push(Bullet('GetIncompleteITMCallRollovers detects the partial rollover.'));
children.push(Bullet('System treats next run as --first-run for that index (skip LEG 1, just buy).'));
children.push(Bullet('Crash-recovery email is sent at the time of the original failure.'));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 9. FAQ
// ════════════════════════════════════════════════════════════════
children.push(H1('9. Frequently Asked Questions'));

children.push(H3('Q: Why does kVegaCrush use NEGATIVE IV shock?'));
children.push(P(
  'A: For long calls, the BAD case is IV going DOWN (vega crush after events). Long vega benefits from ' +
  'IV up — that\'s the GOOD case. The framework explicitly models the bad case by passing a negative ' +
  'IV change into the P&L formula. The result is a negative P&L (loss) which K = abs(P&L) / premium ' +
  'converts to the magnitude.'
));

children.push(H3('Q: What if both indices fail to size?'));
children.push(P(
  'A: The coordinated runner returns empty results, logs an error, and sends no rollover emails. ' +
  'The existing positions (if any) remain untouched. Manual investigation is required — typically ' +
  'this happens if quote data is too stale or the broker connection failed.'
));

children.push(H3('Q: Can I run dynamic K for one index and static for the other?'));
children.push(P(
  'A: Technically yes — the useDynamicK flag is per-index. But the pooled allocation only runs when ' +
  'AT LEAST ONE index has useDynamicK=true. If you mix, the dynamic-K index uses the new pipeline; the ' +
  'static-K index falls back to its own per-index sizing inside the coordinated runner. Not recommended; ' +
  'keep both flags in sync for predictable behavior.'
));

children.push(H3('Q: Why does NIFTY get the tiebreaker instead of BANKNIFTY?'));
children.push(P(
  'A: NIFTY is more liquid (tighter spreads), has lower vega per lot (less stress sensitivity), and ' +
  'smaller per-lot premium (better diversification per rupee). When both options are equally deserving, ' +
  'NIFTY is the safer addition.'
));

children.push(H3('Q: Why 80% threshold for pool round-up, not 90% or 50%?'));
children.push(P(
  'A: 80% strikes a balance between capturing rounding loss (capacity that\'s "almost enough") and ' +
  'limiting over-budget exposure. At 80%, a single lot can put you up to 20 percent over the per-index ' +
  'vol budget — a small price to pay for fuller utilization. At 50% you\'d over-shoot too aggressively; ' +
  'at 100% you\'d waste meaningful capacity on most days.'
));

children.push(H3('Q: What\'s the worst-case loss this system can cause?'));
children.push(P(
  'A: Maximum loss = sum of premium outlays = sum of (cost_per_lot * lots) across all indices. With ' +
  'the 3% per-index cap active, this is bounded at 6% of capital combined (3% per index x 2 indices). ' +
  'In practice it\'s usually 2-4% of capital because the 80% pool rule rarely lets both indices hit ' +
  'their cap simultaneously.'
));

children.push(H3('Q: Does the system rebalance positions intraday or daily?'));
children.push(P(
  'A: NO. Positions are entered once at monthly expiry rollover and held to next expiry. The daily ' +
  'monitor only emits ALERTS — no rebalancing, no position changes. The strategy is intentionally ' +
  '"buy and hold to expiry" with no stoploss.'
));

children.push(H3('Q: When will auto-trim be enabled?'));
children.push(P(
  'A: Auto-trim execution requires careful order placement logic and validation. The infrastructure ' +
  '(detection + email) is ready. The actual sell-down code is intentionally left as alert-only until ' +
  'a few extended dry-run cycles validate the trim trigger and quantity calculation. Until then, manual ' +
  'review and execution is required when the alert fires.'
));

children.push(H3('Q: How is the regime addon different from intraday addon?'));
children.push(P(
  'A: The OLD intraday addon (used for short straddles) looks at TODAY\'s high-low range and adds shock ' +
  'based on absolute thresholds. The NEW regime addon looks at 20-day vs 100-day realized vol RATIO. ' +
  'The regime addon is per-instrument self-calibrating (same 1.5% range means different things for ' +
  'NIFTY vs MIDCPNIFTY) and matches the long monthly call holding period (20 days). The two addons ' +
  'are NOT used simultaneously for ITM calls — only the regime addon applies.'
));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 10. GLOSSARY
// ════════════════════════════════════════════════════════════════
children.push(H1('10. Glossary'));
children.push(makeTable([
  ['Term', 'Definition'],
  ['ITM (In-The-Money)', 'A call option whose strike is below the current spot. Has positive intrinsic value.'],
  ['DTE (Days To Expiry)', 'Trading days until the option expires (excludes weekends and holidays).'],
  ['Greeks', 'Sensitivities of option price to underlying parameters (delta, gamma, vega, theta).'],
  ['Delta', 'Change in option price per Rs change in spot. For ITM calls, around 0.7-0.9.'],
  ['Gamma', 'Change in delta per Rs change in spot. Curvature of P&L vs spot.'],
  ['Vega', 'Change in option price per 1.0 change in IV (decimal). Positive for long calls.'],
  ['Theta', 'Change in option price per calendar day. Negative for long premium positions.'],
  ['IV (Implied Volatility)', 'Vol implied by market premium (decimal, e.g., 0.16 = 16%).'],
  ['vp (vol point)', '0.01 in decimal IV. 8 vp = 0.08 decimal IV change.'],
  ['1 sigma move', 'Expected daily spot move = spot × IV / sqrt(252) (in INR).'],
  ['K (sizing factor)', 'Daily P&L vol per share / option premium. Drives lot sizing.'],
  ['kBase', 'K from -1 sigma adverse spot, no IV change. True daily vol target.'],
  ['kStressMove', 'K from -1.5 sigma adverse spot, no IV change. Fat-tail spot stress.'],
  ['kVegaCrush', 'K from -1 sigma adverse spot + negative IV shock. Vol crush stress.'],
  ['K_use', 'max(kBase, kStressMove, kVegaCrush) — used for actual sizing.'],
  ['K_FLOOR', 'Minimum K (0.20). Clamps very small K from low-Greek scenarios.'],
  ['K_CEILING', 'Maximum K (5.00). Data-quality guard against runaway K.'],
  ['IV Shock', 'Total vol-points added to model an adverse IV regime. Built additively.'],
  ['VIX', 'India VIX index (annualized 30-day expected NIFTY vol).'],
  ['Regime ratio', 'Recent realized vol / Baseline realized vol. Triggers regime addon.'],
  ['Realized vol', 'Annualized standard deviation of daily log returns over a window.'],
  ['Premium-at-risk cap', 'Hard limit on outlay per index (3% of capital).'],
  ['dvpl (Daily Vol Per Lot)', 'K × premium × lotSize. Represents how much daily vol one lot adds.'],
  ['Pool / pooled allocation', 'NIFTY and BANKNIFTY share a combined daily vol budget.'],
  ['Floor lots', 'Per-index minimum from independent vol-target sizing (min 1).'],
  ['Round-up threshold (80%)', 'Pool adds an extra lot if leftover >= 80% of dvpl.'],
  ['Drift', 'Difference between today\'s metric and entry-time value.'],
  ['Cap breach', 'Position outlay exceeds the 3% per-index cap during the hold.'],
], [3000, 6360]));

// ─── BUILD DOCUMENT ────────────────────────────────────────────────
const doc = new Document({
  creator: 'ITM Call User Guide',
  title: 'ITM Call Dynamic K User Guide',
  description: 'Operational reference for the long ITM call buying system',
  styles: {
    default: { document: { run: { font: 'Arial', size: 22 } } },
    paragraphStyles: [
      { id: 'Heading1', name: 'Heading 1', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 32, bold: true, font: 'Arial', color: NAVY },
        paragraph: { spacing: { before: 360, after: 200 }, outlineLevel: 0 } },
      { id: 'Heading2', name: 'Heading 2', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 26, bold: true, font: 'Arial', color: NAVY },
        paragraph: { spacing: { before: 240, after: 120 }, outlineLevel: 1,
          border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: ACCENT, space: 1 } } } },
      { id: 'Heading3', name: 'Heading 3', basedOn: 'Normal', next: 'Normal', quickFormat: true,
        run: { size: 23, bold: true, font: 'Arial', color: ACCENT },
        paragraph: { spacing: { before: 180, after: 80 }, outlineLevel: 2 } },
    ],
  },
  numbering: {
    config: [
      { reference: 'bullets', levels: [{ level: 0, format: LevelFormat.BULLET, text: '•',
        alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
      { reference: 'numbers', levels: [{ level: 0, format: LevelFormat.DECIMAL, text: '%1.',
        alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
    ],
  },
  sections: [{
    properties: {
      page: {
        size: { width: 12240, height: 15840 },
        margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 },
      },
    },
    headers: {
      default: new Header({ children: [new Paragraph({
        alignment: AlignmentType.RIGHT,
        children: [new TextRun({ text: 'ITM Call User Guide v2.0', size: 18, color: '888888' })],
      })] }),
    },
    footers: {
      default: new Footer({ children: [new Paragraph({
        alignment: AlignmentType.CENTER,
        children: [
          new TextRun({ text: 'Page ', size: 18, color: '888888' }),
          new TextRun({ children: [PageNumber.CURRENT], size: 18, color: '888888' }),
          new TextRun({ text: ' of ', size: 18, color: '888888' }),
          new TextRun({ children: [PageNumber.TOTAL_PAGES], size: 18, color: '888888' }),
        ],
      })] }),
    },
    children: children,
  }],
});

Packer.toBuffer(doc).then(buffer => {
  const out = '/Users/ekanshgowda/Documents/Code/Kite_API/docs/ITM_Call_User_Guide.docx';
  fs.writeFileSync(out, buffer);
  console.log('Wrote ' + out + ' (' + buffer.length + ' bytes)');
}).catch(err => { console.error('Error:', err); process.exit(1); });
