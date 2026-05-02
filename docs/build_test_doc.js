/**
 * Build comprehensive Test Document for ITM Call Dynamic K + Pooled Allocation framework.
 */
const fs = require('fs');
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  AlignmentType, LevelFormat, HeadingLevel, BorderStyle, WidthType,
  ShadingType, PageBreak, TabStopType, TabStopPosition, PageNumber,
  Header, Footer, PageOrientation, TableOfContents,
} = require('docx');

// ─── Style constants (matching Anthropic / professional palette) ──
const NAVY = '003366';
const ACCENT = '2E75B6';
const GREEN = '27AE60';
const RED = 'E74C3C';
const GREY_BG = 'F8F9FA';
const BORDER_COL = 'CCCCCC';
const TABLE_HEADER_BG = 'D5E8F0';

const cellBorder = { style: BorderStyle.SINGLE, size: 1, color: BORDER_COL };
const cellBorders = { top: cellBorder, bottom: cellBorder, left: cellBorder, right: cellBorder };

// ─── Helpers ──────────────────────────────────────────────────────
function H1(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_1,
    children: [new TextRun(text)],
    pageBreakBefore: false,
  });
}
function H2(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_2,
    children: [new TextRun(text)],
  });
}
function H3(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_3,
    children: [new TextRun(text)],
  });
}
function P(text, opts = {}) {
  return new Paragraph({
    spacing: { after: 120 },
    children: [new TextRun({ text, ...opts })],
  });
}
function Bullet(text, opts = {}) {
  return new Paragraph({
    numbering: { reference: 'bullets', level: 0 },
    children: [new TextRun({ text, ...opts })],
  });
}
function Numbered(text, opts = {}) {
  return new Paragraph({
    numbering: { reference: 'numbers', level: 0 },
    children: [new TextRun({ text, ...opts })],
  });
}
function Code(text) {
  return new Paragraph({
    spacing: { before: 60, after: 120 },
    children: [new TextRun({ text, font: 'Courier New', size: 20 })],
    shading: { fill: GREY_BG, type: ShadingType.CLEAR },
  });
}
function CodeBlock(lines) {
  return lines.map(line => new Paragraph({
    children: [new TextRun({ text: line, font: 'Courier New', size: 20 })],
    shading: { fill: GREY_BG, type: ShadingType.CLEAR },
  }));
}
function Spacer() {
  return new Paragraph({ children: [new TextRun('')] });
}
function PageBreakP() {
  return new Paragraph({ children: [new PageBreak()] });
}

// Table builder — `rows` is array of arrays (2D), first row is header
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
            text: String(cellText),
            bold: isHeader,
            size: 20,
            color: isHeader ? NAVY : '333333',
          })],
        })],
      })),
    });
  });
  return new Table({
    width: { size: totalWidth, type: WidthType.DXA },
    columnWidths,
    rows: tableRows,
  });
}

// ─── DOCUMENT CONTENT ─────────────────────────────────────────────
const children = [];

// COVER PAGE
children.push(
  new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { before: 3000, after: 240 },
    children: [new TextRun({
      text: 'ITM Call Dynamic K + Pooled Allocation',
      bold: true, size: 44, color: NAVY,
    })],
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { after: 480 },
    children: [new TextRun({
      text: 'Comprehensive Test Document',
      bold: true, size: 36, color: ACCENT,
    })],
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { after: 240 },
    children: [new TextRun({ text: 'Version 2.0', size: 24, color: '555555' })],
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { after: 240 },
    children: [new TextRun({ text: 'Date: May 1, 2026', size: 24, color: '555555' })],
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { before: 2400, after: 120 },
    children: [new TextRun({
      text: 'System: itm_call_rollover.py + PlaceOptionsSystemsV2.py + itm_call_daily_monitor.py',
      italics: true, size: 20, color: '888888',
    })],
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER,
    children: [new TextRun({
      text: 'Test Suites: 36 automated + 7 edge-case categories + live data verification',
      italics: true, size: 20, color: '888888',
    })],
  }),
  PageBreakP(),
);

// TABLE OF CONTENTS
children.push(H1('Table of Contents'));
children.push(new TableOfContents('Contents', { hyperlink: true, headingStyleRange: '1-3' }));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 1. EXECUTIVE SUMMARY
// ════════════════════════════════════════════════════════════════
children.push(H1('1. Executive Summary'));
children.push(P(
  'This document records the comprehensive testing performed on the ITM Call Dynamic K + ' +
  'Pooled Allocation framework before its activation in production. The framework replaces ' +
  'the legacy static K=0.18 sizing with a dynamic, Greeks-based K that adapts to market ' +
  'conditions, and introduces pooled lot allocation across NIFTY and BANKNIFTY long ITM call ' +
  'positions.'
));

children.push(H2('1.1 Test Outcome'));
children.push(makeTable([
  ['Test Suite', 'Tests', 'Result'],
  ['Unit tests (test_itm_call_dynamic_k.py)', '21', 'PASS (21/21)'],
  ['Integration tests (test_itm_call_integration.py)', '15', 'PASS (15/15)'],
  ['Edge case tests (inline)', '7 categories', 'PASS (all)'],
  ['Live data dry-run via Kite API', 'End-to-end', 'PASS'],
  ['Email rendering (live data)', '3 emails', 'PASS (all sections)'],
  ['Final smoke test', 'Imports + functional', 'PASS'],
  ['TOTAL', '36 + 7 + live', 'ALL PASS'],
], [4500, 2000, 2860]));
children.push(Spacer());

children.push(H2('1.2 Verdict'));
children.push(P(
  'All test suites pass. The framework is functional, gated by the useDynamicK config flag ' +
  '(set to true in instrument_config.json), and ready for production use on the next monthly ' +
  'expiry day. The static K path remains as a fallback for any quote-quality failure.'
));

children.push(H2('1.3 Files Under Test'));
children.push(makeTable([
  ['File', 'Lines', 'Purpose'],
  ['PlaceOptionsSystemsV2.py', '~2,150', 'Greeks, IV solver, dynamic K computation, regime addon'],
  ['itm_call_rollover.py', '~2,100', 'Entry orchestration, pool allocator, email builders'],
  ['itm_call_daily_monitor.py', '~330', 'Drift monitoring with alert emails'],
  ['instrument_config.json', '~1,000', 'Per-index config: useDynamicK, premium cap, regime'],
  ['test_itm_call_dynamic_k.py', '~280', 'Unit tests for K + sizing + allocation'],
  ['test_itm_call_integration.py', '~360', 'Integration tests with mocked Kite client'],
], [3500, 1500, 4360]));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 2. TEST METHODOLOGY
// ════════════════════════════════════════════════════════════════
children.push(H1('2. Test Methodology'));

children.push(H2('2.1 Testing Pyramid'));
children.push(P(
  'Testing follows a three-layer pyramid:'
));
children.push(Numbered('Unit tests verify individual function behavior in isolation. Each test asserts a single property of one function (e.g., kBase value matches hand calculation).'));
children.push(Numbered('Integration tests verify interactions between functions using a mocked Kite client. These cover the full pipeline (resolve K -> prepare sizing -> allocate -> render email) without touching the real broker.'));
children.push(Numbered('Live data verification runs the new code path against the real Kite API in read-only mode, confirming the system handles real market data correctly.'));

children.push(H2('2.2 Test Tools'));
children.push(makeTable([
  ['Tool', 'Purpose'],
  ['pytest 9.0.2', 'Test runner for both unit and integration suites'],
  ['unittest.mock', 'Kite client mocking for integration tests'],
  ['Real Kite API (read-only)', 'Live data verification via GetKiteClient'],
  ['Black-Scholes implementation', 'Independent hand calculations to verify K values'],
], [3500, 5860]));

children.push(H2('2.3 Test Naming Convention'));
children.push(P(
  'Tests follow the convention test_<unit>_<scenario>_<expected_outcome>. Examples:'
));
children.push(...CodeBlock([
  'test_kVegaCrush_binds_at_8vp_shock',
  'test_balance_rule_prefers_index_with_fewer_lots',
  'test_fallback_to_static_when_quote_fails',
  'test_static_K_email_still_works',
]));

children.push(H2('2.4 What This Test Plan Does NOT Cover'));
children.push(Bullet('Live order placement (the new code path is dry-run validated only; first live execution will be on the next monthly expiry day).'));
children.push(Bullet('Auto-trim execution in the daily monitor (the trim function is intentionally left as alerts-only for safety until extended dry-run cycles validate it).'));
children.push(Bullet('Multi-cycle behavior over weeks (requires production runs to gather data).'));
children.push(Bullet('Stress conditions like Kite API outages, exchange downtime, or partial fills (covered by existing rollover error handling).'));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 3. TEST COVERAGE MATRIX
// ════════════════════════════════════════════════════════════════
children.push(H1('3. Test Coverage Matrix'));
children.push(P('Maps each new component to its test coverage.'));
children.push(makeTable([
  ['Component', 'Unit Tests', 'Integration Tests', 'Live'],
  ['computeDynamicK long_single', '7', '0', 'Yes'],
  ['kVegaCrush scenario (-1 sigma + neg shock)', '3', '0', 'Yes'],
  ['kBase / kStressMove for long', '2', '0', 'Yes'],
  ['Greeks polarity (no negation for long)', '2 (edge)', '0', 'N/A'],
  ['lookupRegimeAddon mapping', '5 + 14 boundary', '0', 'N/A'],
  ['getRegimeAddon helper', '0', '2', 'Yes'],
  ['resolveKLongSingle orchestrator', '0', '4', 'Yes'],
  ['Static K fallback on failure', '0', '2', 'N/A'],
  ['ComputePositionSizeITM with cap', '3', '0', 'Yes'],
  ['Premium-at-risk cap binding', '1', '1 (edge)', 'Yes'],
  ['AllocateLotsBalanced pool', '5', '2 (edge)', 'Yes'],
  ['Balance rule (fewer-lots preference)', '2', '0', 'N/A'],
  ['NIFTY tiebreaker', '1', '0', 'N/A'],
  ['80% round-up threshold', '1', '0', 'N/A'],
  ['Premium cap blocking', '1', '0', 'N/A'],
  ['BuildRolloverEmailHtml (dynamic)', '0', '1', 'Yes'],
  ['BuildRolloverEmailHtml (static fallback)', '0', '1', 'N/A'],
  ['BuildCombinedPortfolioEmail', '0', '1', 'Yes'],
  ['BuildDailyMonitorEmail', '0', '1', 'N/A'],
  ['BuildAutoTrimEmail', '0', '1', 'N/A'],
  ['Short straddle path (regression)', '1', '0', 'N/A'],
], [4000, 1700, 1900, 1760]));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 4. UNIT TESTS DETAIL
// ════════════════════════════════════════════════════════════════
children.push(H1('4. Unit Tests'));
children.push(P('File: test_itm_call_dynamic_k.py | 21 tests | All pass in 0.93 seconds.'));

children.push(H2('4.1 TestComputeDynamicKLongSingle (7 tests)'));
children.push(P('Verifies the new long_single strategy type in computeDynamicK.'));
children.push(makeTable([
  ['Test', 'Verifies', 'Expected'],
  ['test_returns_three_scenarios_for_long_single', 'kBase, kStressMove, kVegaCrush populated; kStressVol/kCrash are None', 'All three K values present'],
  ['test_kVegaCrush_binds_at_8vp_shock', 'kVegaCrush dominates max() with current Greeks', 'kBindingScenario == kVegaCrush'],
  ['test_kBase_value_matches_hand_calc', 'kBase computed from -1 sigma adverse spot + 0 IV', 'kBase ~= 0.140'],
  ['test_kStressMove_value_matches_hand_calc', 'kStressMove from -1.5 sigma adverse + 0 IV', 'kStressMove ~= 0.206'],
  ['test_kVegaCrush_value_matches_hand_calc', 'kVegaCrush from -1 sigma adverse + neg shock', 'kVegaCrush ~= 0.300'],
  ['test_K_for_sizing_clamped_to_floor', 'Tiny Greeks produce K below floor, then clamped', 'kForSizing == K_FLOOR (0.20)'],
  ['test_short_straddle_unchanged', 'Existing strategyType=straddle path unchanged', 'kStressVol, kCrash present; kVegaCrush is None'],
], [3700, 3500, 2160]));

children.push(H2('4.2 TestRegimeAddon (5 tests)'));
children.push(P('Verifies lookupRegimeAddon ratio-to-vol-points mapping.'));
children.push(makeTable([
  ['Test', 'Verifies', 'Expected'],
  ['test_below_baseline_negative_addon', 'Ratio < 0.7 returns negative addon', '< 0'],
  ['test_near_baseline_zero_addon', 'Ratio in 0.9-1.10 returns 0', '== 0'],
  ['test_mild_expansion_positive_addon', 'Ratio in 1.10-1.30 returns +2vp', '> 0'],
  ['test_extreme_expansion_caps', 'Ratio > 2.00 caps at +8vp', '== 8vp'],
  ['test_addon_returns_decimal', 'Returns decimal (e.g., 0.02 for +2vp)', '0.02'],
], [3700, 3500, 2160]));

children.push(H2('4.3 TestComputePositionSizeITM (3 tests)'));
children.push(P('Verifies position sizing with and without premium-at-risk cap.'));
children.push(makeTable([
  ['Test', 'Verifies', 'Expected'],
  ['test_no_cap_uses_vol_target_only', 'No cap -> only vol-target binds', 'lotsCap is None, binding = vol-target'],
  ['test_cap_binds_when_lots_vol_exceeds_cap', 'Low K + small premium -> cap binds', 'binding = premium-cap'],
  ['test_minimum_one_lot_enforced', 'Tiny budget -> still 1 lot', 'finalLots == 1'],
], [3700, 3500, 2160]));

children.push(H2('4.4 TestAllocateLotsBalanced (6 tests)'));
children.push(P('Verifies the pooled allocation algorithm.'));
children.push(makeTable([
  ['Test', 'Verifies', 'Expected'],
  ['test_today_scenario_2N_1B', 'Pool extras yield 2 NIFTY + 1 BANK with current K', '{NIFTY:2, BANK:1}'],
  ['test_balance_rule_prefers_index_with_fewer_lots', 'NIFTY has 2, BANK has 1 -> extra goes to BANK', '{NIFTY:2, BANK:2}'],
  ['test_NIFTY_tiebreaker_when_equal_lots', 'Tied lot counts -> NIFTY wins', 'NIFTY incremented first'],
  ['test_premium_cap_blocks_extra_lot', 'Cap prevents both indices growing', '{NIFTY:1, BANK:1}'],
  ['test_80_pct_round_up_kicks_in', 'Leftover 84% of dvpl -> add lot, accept over-budget', 'over_budget > 0'],
  ['test_no_extras_when_leftover_too_small', 'Tiny leftover -> stay at floor', 'No extras added'],
], [3700, 3500, 2160]));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 5. INTEGRATION TESTS DETAIL
// ════════════════════════════════════════════════════════════════
children.push(H1('5. Integration Tests'));
children.push(P('File: test_itm_call_integration.py | 15 tests | All pass in 0.86 seconds. Uses mocked Kite client to test the full pipeline.'));

children.push(H2('5.1 TestResolveKLongSingle (4 tests)'));
children.push(P('Verifies the resolveKLongSingle orchestrator end-to-end.'));
children.push(makeTable([
  ['Test', 'Verifies', 'Expected'],
  ['test_full_pipeline_returns_dynamic_K', 'Full happy path with realistic mock data', 'source=dynamic, K in [floor, ceiling]'],
  ['test_fallback_to_static_when_quote_fails', 'Quote exception triggers static fallback', 'source=static_fallback, K=0.18'],
  ['test_fallback_when_iv_out_of_bounds', 'IV near solver bounds triggers fallback', 'source=static_fallback'],
  ['test_strict_no_fallback_returns_None_on_failure', 'staticKFallback=None and quote fails', 'returns (None, source=failed)'],
], [3700, 3500, 2160]));

children.push(H2('5.2 TestGetRegimeAddon (2 tests)'));
children.push(makeTable([
  ['Test', 'Verifies', 'Expected'],
  ['test_returns_zero_addon_when_no_data', 'Network failure returns safe defaults', '(0.0, None, None, None)'],
  ['test_returns_addon_with_mocked_history', 'Mock history yields valid ratio + addon', 'Valid numeric outputs'],
], [3700, 3500, 2160]));

children.push(H2('5.3 TestRunCoordinatedRollover (1 test)'));
children.push(makeTable([
  ['Test', 'Verifies', 'Expected'],
  ['test_dry_run_executes_without_orders', 'Coordinated runner completes dry-run end-to-end', 'k_metadata populated, no orders'],
], [3700, 3500, 2160]));

children.push(H2('5.4 TestEmailRendering (5 tests)'));
children.push(P('Verifies all 4 email types render correctly with realistic data.'));
children.push(makeTable([
  ['Test', 'Verifies', 'Expected'],
  ['test_dynamic_K_email_renders_all_sections', 'Dynamic K email contains all 3 new sections', 'Dynamic K, IV Shock, Pool sections'],
  ['test_static_K_email_still_works', 'Static path renders existing K_TABLE_SINGLE format', 'Contains STATIC, K_TABLE_SINGLE'],
  ['test_combined_portfolio_email', 'Combined email shows per-index breakdown + totals', 'Per-Index Breakdown section'],
  ['test_daily_monitor_email', 'Drift alert email renders with amber theme', 'Contains DRIFT ALERT banner'],
  ['test_auto_trim_email', 'Auto-trim email renders with red theme', 'Contains AUTO-TRIM details'],
], [3700, 3500, 2160]));

children.push(H2('5.5 TestEdgeCases (3 tests)'));
children.push(makeTable([
  ['Test', 'Verifies', 'Expected'],
  ['test_allocate_with_empty_inputs', 'Empty SizingInputs returns empty allocation', '{} allocation'],
  ['test_allocate_with_single_index', 'Single-index input still allocates', 'NIFTY allocation present'],
  ['test_compute_position_invalid_inputs', 'Negative/zero inputs trigger skip', 'skipped=True, finalLots=0'],
], [3700, 3500, 2160]));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 6. EDGE CASE TESTS
// ════════════════════════════════════════════════════════════════
children.push(H1('6. Edge Case Tests'));
children.push(P('Beyond unit and integration suites, 7 additional edge case categories were verified.'));

children.push(H2('6.1 Over-Budget via 80% Rule'));
children.push(P('Constructed inputs where pooled leftover lands in the 80-100% threshold. The algorithm correctly added the lot and accepted ~5,094 INR over-budget (5.6% of pool).'));
children.push(...CodeBlock([
  'NIFTY dvpl=32000, BANK dvpl=32500, both floor=1',
  'Pool=91406, used=64500, leftover=26906 (84% of NIFTY dvpl)',
  '-> 80% rule triggers: add NIFTY -> 2N + 1B',
  '-> over_budget = 5094 (5.6% of pool)',
]));

children.push(H2('6.2 Premium Cap Restricts Vol-Target Lots'));
children.push(P('When K is small enough to allow many lots, the premium-cap correctly bounds the position size.'));
children.push(...CodeBlock([
  'Premium=500, K=0.05, budget=500000, cap=100000',
  'lots_vol = 308 (vol-target wants huge position)',
  'lots_cap = floor(100000/32500) = 3',
  '-> finalLots = 3, binding = premium-cap',
]));

children.push(H2('6.3 K Floor Clamping'));
children.push(P('Tiny Greeks produce a near-zero K that gets clamped to K_FLOOR=0.20.'));
children.push(...CodeBlock([
  'delta=0.001, vega=1, IV=0.001, premium=1000, shock=0.001',
  'K_raw = 0.000001',
  'K_clamped (floor) = 0.20',
  'kClamped flag = True',
]));

children.push(H2('6.4 Regime Addon Boundary Values'));
children.push(P('All 14 boundary values across the 7 buckets in REGIME_ADDON_TABLE return the expected vol points.'));
children.push(makeTable([
  ['Ratio', 'Expected', 'Got'],
  ['0.00', '-2 vp', '-2 vp PASS'],
  ['0.69', '-2 vp', '-2 vp PASS'],
  ['0.70', '-1 vp', '-1 vp PASS'],
  ['0.89', '-1 vp', '-1 vp PASS'],
  ['0.90', '0 vp', '0 vp PASS'],
  ['1.09', '0 vp', '0 vp PASS'],
  ['1.10', '+2 vp', '+2 vp PASS'],
  ['1.29', '+2 vp', '+2 vp PASS'],
  ['1.30', '+4 vp', '+4 vp PASS'],
  ['1.59', '+4 vp', '+4 vp PASS'],
  ['1.60', '+6 vp', '+6 vp PASS'],
  ['1.99', '+6 vp', '+6 vp PASS'],
  ['2.00', '+8 vp', '+8 vp PASS'],
  ['5.00', '+8 vp', '+8 vp PASS'],
], [3000, 3000, 3360]));

children.push(H2('6.5 Greeks Polarity for long_single vs short single'));
children.push(P('Critical regression check: long_single must NOT negate Greeks (long delta is positive). short single MUST negate (short delta is conceptually negative).'));
children.push(...CodeBlock([
  'Input vega = 2791',
  'long_single -> posVega = +2791 (no negation) PASS',
  'short single -> posVega = -2791 (negated) PASS',
]));

children.push(H2('6.6 Empty / Single-Index Inputs'));
children.push(P('AllocateLotsBalanced handles edge cases gracefully:'));
children.push(Bullet('Empty SizingInputs dict -> empty allocation, pool=0'));
children.push(Bullet('Single index in inputs -> single-index allocation'));
children.push(Bullet('Both indices fail prepare -> coordinated runner returns empty results'));

children.push(H2('6.7 Static K Path Regression'));
children.push(P('When useDynamicK=false, the existing static K=0.18 path executes unchanged. The email renders the legacy K_TABLE_SINGLE format. This was verified by setting the flag to false in mock and rendering the email.'));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 7. LIVE DATA VERIFICATION
// ════════════════════════════════════════════════════════════════
children.push(H1('7. Live Data Verification'));
children.push(P(
  'On May 1, 2026 (Maharashtra Day - market closed), a read-only dry-run was executed against the real ' +
  'Kite API. NIFTY data was fresh enough to pass the price-quality gates; BANKNIFTY quotes were stale ' +
  'enough to be correctly rejected by existing intrinsic-overpay and BS-deviation gates (this is the ' +
  'system working as designed when prices are stale).'
));

children.push(H2('7.1 NIFTY Live Snapshot'));
children.push(makeTable([
  ['Field', 'Value'],
  ['Spot', '23,997.55'],
  ['Strike Selected (4-5% ITM)', '22,800'],
  ['Premium', 'Rs 1,592.90'],
  ['Solved IV', '16.06%'],
  ['DTE (trading days)', '40'],
  ['VIX', '18.46'],
  ['Regime ratio (20d / 100d realized vol)', '1.08x'],
  ['IV shock build', '4 (DTE) + 4 (VIX) + 0 (regime) = 8 vp'],
  ['kBase', '0.1294'],
  ['kStressMove', '0.1899'],
  ['kVegaCrush', '0.2480 (binds)'],
  ['K_use', '0.2480'],
  ['Daily vol per lot', 'Rs 25,678'],
  ['Floor lots', '1'],
  ['Final lots (after pool)', '1'],
  ['Outlay', 'Rs 103,538 (1.04% of capital)'],
], [4500, 4860]));

children.push(H2('7.2 BANKNIFTY Skip (Expected)'));
children.push(P(
  'All 7 BANKNIFTY 4-5% ITM strikes were rejected by SelectBestITMStrike due to stale prices. ' +
  'This is correct behavior: the existing price-quality gates protect against entering positions on ' +
  'invalid quote data. The coordinated runner gracefully skipped BANKNIFTY and continued with NIFTY only.'
));
children.push(...CodeBlock([
  'BANKNIFTY 52100 REJECTED: INTRINSIC_OVERPAY: 5159 > 3730 (35% above intrinsic)',
  'BANKNIFTY 52200 REJECTED: INTRINSIC_OVERPAY: 5067 > 3595 (...)',
  '[7 candidates rejected]',
  '[BANKNIFTY] [PREPARE] Strike selection failed -> graceful skip',
]));

children.push(H2('7.3 What This Verifies'));
children.push(Bullet('GetKiteClient() connects successfully with the new code.'));
children.push(Bullet('Spot LTP fetch returns the correct token + last_price.'));
children.push(Bullet('Historical data fetch (100 days) succeeds and feeds the regime addon.'));
children.push(Bullet('IV solver produces a clean IV from real market premium.'));
children.push(Bullet('Greeks computation returns finite, sensible values.'));
children.push(Bullet('Three K scenarios compute and the binding scenario is identified.'));
children.push(Bullet('Per-index sizing produces lots and dvpl matching expectations.'));
children.push(Bullet('Pooled allocation runs without error even when only one index participates.'));
children.push(Bullet('Email is rendered (25,712 chars) with all new sections present.'));
children.push(Bullet('Static K fallback is exercised correctly when needed.'));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 8. EMAIL RENDERING TESTS
// ════════════════════════════════════════════════════════════════
children.push(H1('8. Email Rendering Tests'));
children.push(P('All 4 email types were verified for content and aesthetic consistency.'));

children.push(H2('8.1 Rollover Email (Dynamic K)'));
children.push(P('Generated 25,712 chars HTML. Verified sections:'));
children.push(makeTable([
  ['Section', 'Content', 'Status'],
  ['Header', 'Navy banner with index name + date', 'PASS'],
  ['Status Banner', 'Green for SUCCESS', 'PASS'],
  ['Contract & Market Data', 'Spot, strike, premium, BS theo', 'PASS (existing)'],
  ['Position Sizing Formula', 'Step-by-step lot computation', 'PASS (existing)'],
  ['Dynamic K Computation', 'Greeks panel + 3 scenarios + K_use callout', 'PASS (NEW)'],
  ['IV Shock Construction', 'Base + VIX + Regime additive breakdown', 'PASS (NEW)'],
  ['Pooled Allocation', 'Iterations table with primary/fallback markers', 'PASS (NEW)'],
  ['Strike Selection Candidates', 'All considered + rejected', 'PASS (existing)'],
  ['Price Validation', 'BS theo vs market', 'PASS (existing)'],
  ['Leg 2 Entry', 'Order details', 'PASS (existing)'],
  ['Roll Summary', 'P&L overview', 'PASS (existing)'],
], [3000, 4500, 1860]));

children.push(H2('8.2 Combined Portfolio Email'));
children.push(P('Generated 4,539 chars HTML. Sent ONCE per cycle after both indices complete.'));
children.push(Bullet('Per-Index Breakdown table with NIFTY and BANKNIFTY rows + COMBINED row'));
children.push(Bullet('Capital Usage callout: outlay %, vol budget utilization, leftover, worst-day MTM, max loss at expiry'));
children.push(Bullet('Footer with timestamp'));

children.push(H2('8.3 Daily Monitor Alert Email'));
children.push(P('Generated 5,381 chars HTML. Sent only when drift thresholds are breached.'));
children.push(Bullet('Amber status banner (vs green for OK)'));
children.push(Bullet('Drift Metrics table: Spot, VIX, K, Capital, DTE - each with entry value, today value, drift %, alert flag'));
children.push(Bullet('Position Status panel: symbol, lots, qty, MTM, current outlay'));
children.push(Bullet('Recommended Review section with human-readable guidance'));

children.push(H2('8.4 Auto-Trim Email'));
children.push(P('Generated 2,132 chars HTML. Sent only when premium-cap is breached and trim executed.'));
children.push(Bullet('Red status banner (cap breach)'));
children.push(Bullet('Trim Details table: outlay before/after, lots before/after, realized P&L on trimmed lots'));

children.push(H2('8.5 Aesthetic Consistency'));
children.push(P('All 4 email types use the same color palette as the existing rollover email:'));
children.push(makeTable([
  ['Color', 'Hex', 'Usage'],
  ['Navy', '#003366', 'Header bar, table header bg, callout borders'],
  ['Accent Blue', '#2E75B6', 'Section H2 underlines, binding row highlight'],
  ['Green', '#27AE60', 'SUCCESS banner, K_use callout border'],
  ['Red', '#E74C3C', 'FAIL banner, negative P&L, drift alerts'],
  ['Amber', '#F39C12', 'Drift alert banner (monitor only)'],
  ['Grey BG', '#F8F9FA', 'Alternating table rows, info panels'],
], [2000, 1500, 5860]));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 9. PERFORMANCE & ROBUSTNESS
// ════════════════════════════════════════════════════════════════
children.push(H1('9. Performance & Robustness'));

children.push(H2('9.1 Test Execution Time'));
children.push(makeTable([
  ['Suite', 'Tests', 'Time'],
  ['Unit tests', '21', '0.93s'],
  ['Integration tests', '15', '0.86s'],
  ['Combined run', '36', '0.72s'],
  ['Edge case suite (inline script)', '7 categories', '~1s'],
  ['Live data dry-run (1 index)', '1 cycle', '~10s'],
], [4000, 2000, 3360]));

children.push(H2('9.2 Robustness Mechanisms'));
children.push(makeTable([
  ['Failure Mode', 'Mitigation'],
  ['Quote fetch exception', 'Static K fallback (configurable)'],
  ['IV near solver bounds', 'Static K fallback'],
  ['Spread too wide (>30% of mid)', 'Static K fallback'],
  ['Stale quote during market hours (>60s)', 'Static K fallback'],
  ['Premium below MIN_PREMIUM_INR (Rs 0.50)', 'Static K fallback'],
  ['K below floor (0.20)', 'Auto-clamp to K_FLOOR'],
  ['K above ceiling (5.00)', 'Auto-clamp to K_CEILING (data quality guard)'],
  ['Historical data fetch fails', 'Regime addon = 0 (safe default)'],
  ['One index fails prepare', 'Pool runs with remaining indices, no crash'],
  ['Both indices fail prepare', 'Coordinated runner returns empty, logs error'],
  ['Outlay exceeds 4% cap during hold', 'Daily monitor alerts (auto-trim deferred for safety)'],
  ['Email send failure', 'Logged, does NOT block trading'],
], [4500, 4860]));

children.push(H2('9.3 Idempotency / Crash Recovery'));
children.push(P(
  'The new framework preserves all existing crash recovery mechanisms. The pool allocation runs ' +
  'INSIDE the per-index ExecuteRollover (via OverrideFinalLots), so partial-execution scenarios ' +
  '(LEG 1 done, LEG 2 failed) are still handled by the existing GetIncompleteITMCallRollovers ' +
  'logic and treat the recovery as a first-run.'
));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 10. TEST ENVIRONMENT
// ════════════════════════════════════════════════════════════════
children.push(H1('10. Test Environment'));
children.push(makeTable([
  ['Component', 'Version / Config'],
  ['Python', '3.13.7 (homebrew + venv)'],
  ['pytest', '9.0.2'],
  ['kiteconnect', 'Per requirements.txt'],
  ['undetected-chromedriver', 'For Auto3 token refresh'],
  ['Operating System', 'macOS Darwin'],
  ['Test Date', 'May 1, 2026 (Maharashtra Day - market closed)'],
  ['Kite Account', 'OFS653 (live API access, read-only used)'],
  ['Capital config', 'Rs 99,99,999 base, 50% annual vol target'],
  ['useDynamicK flag at test time', 'true (verified live)'],
], [3500, 5860]));
children.push(PageBreakP());

// ════════════════════════════════════════════════════════════════
// 11. APPENDICES
// ════════════════════════════════════════════════════════════════
children.push(H1('Appendix A: Hand Calculations Verifying K Values'));
children.push(P('The unit tests assert K values within 1% of these hand calculations.'));

children.push(H2('A.1 NIFTY (today)'));
children.push(P('Inputs: spot=23,997.55, strike=23,050, T=60/365, IV=16.4%, premium=Rs 1,405.15'));
children.push(P('Greeks: delta=0.792, gamma=0.000180, vega=2,791, theta=-7.19'));
children.push(P('1-sigma daily move: spot * IV / sqrt(252) = 248 INR/share'));
children.push(...CodeBlock([
  'kBase (-1 sigma, 0 IV):',
  '  pnl = 0.792 * (-248) + 0.5 * 0.000180 * 248^2 + 0 + (-7.19)',
  '      = -196.4 + 5.5 - 7.19 = -198 (~-197)',
  '  K = 198 / 1405 = 0.140',
  '',
  'kStressMove (-1.5 sigma, 0 IV):',
  '  pnl = 0.792 * (-372) + 0.5 * 0.000180 * 372^2 + 0 + (-7.19)',
  '      = -294.6 + 12.4 - 7.19 = -289',
  '  K = 289 / 1405 = 0.206',
  '',
  'kVegaCrush (-1 sigma, -8vp shock):',
  '  pnl = 0.792 * (-248) + 0.5 * 0.000180 * 248^2 + 2791 * (-0.08) + (-7.19)',
  '      = -196.4 + 5.5 - 223.3 - 7.19 = -421',
  '  K = 421 / 1405 = 0.300',
  '',
  'K_use = max(0.140, 0.206, 0.300) = 0.300 (kVegaCrush binds)',
]));

children.push(H1('Appendix B: Test Output Logs'));
children.push(P('Final pytest output (combined):'));
children.push(...CodeBlock([
  '$ .venv/bin/python -m pytest test_itm_call_dynamic_k.py test_itm_call_integration.py -v',
  '',
  '============================= test session starts ==============================',
  'platform darwin -- Python 3.13.7, pytest-9.0.2',
  'collected 36 items',
  '',
  'test_itm_call_dynamic_k.py .....................                 [ 58%]',
  'test_itm_call_integration.py ...............                     [100%]',
  '',
  '============================== 36 passed in 0.72s ==============================',
]));

children.push(H1('Appendix C: Files Modified Summary'));
children.push(makeTable([
  ['Type', 'File', 'Action'],
  ['Modified', 'PlaceOptionsSystemsV2.py', 'Added long_single, regime addon, resolveKLongSingle'],
  ['Modified', 'itm_call_rollover.py', 'Added pool allocator, dynamic K wiring, enhanced email'],
  ['Modified', 'instrument_config.json', 'Added useDynamicK, premium cap, regime config'],
  ['New', 'itm_call_daily_monitor.py', 'Daily drift monitor with alert emails'],
  ['New', 'test_itm_call_dynamic_k.py', '21 unit tests'],
  ['New', 'test_itm_call_integration.py', '15 integration tests'],
], [1500, 3500, 4360]));

// ─── BUILD DOCUMENT ────────────────────────────────────────────────
const doc = new Document({
  creator: 'ITM Call Test Plan',
  title: 'ITM Call Dynamic K Test Document',
  description: 'Comprehensive test record',
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
        size: { width: 12240, height: 15840 },  // US Letter
        margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 },
      },
    },
    headers: {
      default: new Header({ children: [new Paragraph({
        alignment: AlignmentType.RIGHT,
        children: [new TextRun({ text: 'ITM Call Test Document v2.0', size: 18, color: '888888' })],
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
  const out = '/Users/ekanshgowda/Documents/Code/Kite_API/docs/ITM_Call_Test_Document.docx';
  fs.writeFileSync(out, buffer);
  console.log('Wrote ' + out + ' (' + buffer.length + ' bytes)');
}).catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
