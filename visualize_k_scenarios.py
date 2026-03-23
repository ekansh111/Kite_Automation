"""
Visualize dynamic k values across different market conditions.

Generates a 2×2 dashboard:
  1. All 4 k scenarios vs DTE (at current VIX)
  2. kForSizing heatmap: DTE × VIX level
  3. kForSizing vs VIX at different DTEs (line chart)
  4. Binding scenario map: which scenario dominates at each DTE × VIX

Usage:
    python3 visualize_k_scenarios.py
"""

import math
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Patch
import sys
sys.path.insert(0, "/Users/ekanshgowda/Documents/Code/Kite_API")

import PlaceOptionsSystemsV2 as V2


# ── Parameters ──
SPOT_NIFTY = 22600       # approximate current spot
LOT_SIZE = 75
IV_BASE = 0.14           # "normal" IV (14%)
IV_ELEVATED = 0.30       # elevated IV
IV_CRISIS = 0.50         # crisis IV

DTE_RANGE = [1, 2, 3, 4, 5, 6, 7]
VIX_RANGE = np.arange(10, 42, 1)  # 10 to 41
IV_FOR_VIX = {            # rough IV assumption for each VIX regime
    (0, 14): 0.12,
    (14, 18): 0.15,
    (18, 24): 0.22,
    (24, 30): 0.35,
    (30, 42): 0.50,
}


def iv_for_vix(vix):
    """Map VIX level to a plausible ATM IV (rough heuristic)."""
    for (lo, hi), iv in IV_FOR_VIX.items():
        if lo <= vix < hi:
            return iv
    return 0.50


def compute_k_grid(spot, lotSize):
    """Compute k values across all DTE × VIX combinations."""
    results = {}  # (dte, vix) -> dict with all 4 scenarios + binding

    for dte in DTE_RANGE:
        T = dte / 365.0
        baseIvShock = V2.lookupIvShock(dte)

        for vix in VIX_RANGE:
            vix = float(vix)
            iv = iv_for_vix(vix)

            # VIX add-on
            vixAddon = 0.0
            for lo, hi, addon in V2.VIX_ADDON_TABLE:
                if lo <= vix < hi:
                    vixAddon = addon / 100.0
                    break

            # Use 0 intraday add-on for the grid (separate chart for that)
            intradayAddon = 0.0

            # Total IV shock (capped)
            ivShock = min(baseIvShock + vixAddon + intradayAddon,
                          V2.IV_SHOCK_CAP_VP / 100.0)

            # Compute Greeks
            ceGreeks = V2.bsGreeks(spot, spot, T, iv, "CE")
            peGreeks = V2.bsGreeks(spot, spot, T, iv, "PE")

            if ceGreeks is None or peGreeks is None:
                continue

            cePremium = V2.bsPrice(spot, spot, T, iv, "CE")
            pePremium = V2.bsPrice(spot, spot, T, iv, "PE")
            combinedPremium = cePremium + pePremium

            if combinedPremium <= 0:
                continue

            r = V2.computeDynamicK(
                ceGreeks, peGreeks, iv, iv, spot, combinedPremium,
                lotSize, "straddle", ivShockAbsolute=ivShock,
            )

            if r is not None:
                results[(dte, vix)] = r

    return results


def compute_premium_grid(spot, lotSize):
    """Compute combined ATM straddle premium across DTE × VIX for capital-per-lot chart."""
    premiums = {}
    for dte in DTE_RANGE:
        T = dte / 365.0
        for vix in VIX_RANGE:
            iv = iv_for_vix(float(vix))
            ce = V2.bsPrice(spot, spot, T, iv, "CE")
            pe = V2.bsPrice(spot, spot, T, iv, "PE")
            if ce is not None and pe is not None:
                premiums[(dte, float(vix))] = ce + pe
    return premiums


def plot_dashboard(results, spot):
    """Generate 3×2 dashboard."""
    fig, axes = plt.subplots(3, 2, figsize=(20, 22))
    fig.suptitle(f"Dynamic K Scenario Dashboard — NIFTY ATM Short Straddle (spot≈{spot})",
                 fontsize=14, fontweight="bold", y=0.98)

    # ── Chart 1: All 4 scenarios vs DTE at different VIX levels ──
    ax1 = axes[0, 0]
    vix_samples = [12, 20, 27, 35]
    colors = ["#2ecc71", "#3498db", "#e67e22", "#e74c3c"]
    scenario_keys = ["kBase", "kStressMove", "kStressVol", "kCrash"]
    linestyles = ["-.", "--", ":", "-"]

    for scenario, ls in zip(scenario_keys, linestyles):
        for vix_val, color in zip(vix_samples, colors):
            ys = []
            xs = []
            for dte in DTE_RANGE:
                key = (dte, float(vix_val))
                if key in results:
                    ys.append(results[key][scenario])
                    xs.append(dte)
            if xs:
                label = f"{scenario} (VIX={vix_val})" if scenario == "kCrash" else None
                alpha = 1.0 if scenario == "kCrash" else 0.35
                lw = 2.5 if scenario == "kCrash" else 1.2
                ax1.plot(xs, ys, ls, color=color, alpha=alpha, linewidth=lw, label=label)

    # Add VIX color legend manually
    for vix_val, color in zip(vix_samples, colors):
        regime = {12: "calm", 20: "elevated", 27: "stressed", 35: "panic"}[vix_val]
        ax1.plot([], [], "-", color=color, linewidth=2.5,
                 label=f"VIX={vix_val} ({regime})")

    # Add scenario style legend
    for scenario, ls in zip(scenario_keys, linestyles):
        ax1.plot([], [], ls, color="gray", linewidth=1.5, label=scenario)

    ax1.set_xlabel("DTE (trading days)")
    ax1.set_ylabel("k value")
    ax1.set_title("All 4 Scenarios vs DTE at Different VIX Levels")
    ax1.legend(fontsize=7, loc="upper right", ncol=2)
    ax1.set_xticks(DTE_RANGE)
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(0, 4.0)

    # ── Chart 2: kForSizing heatmap (DTE × VIX) ──
    ax2 = axes[0, 1]
    heatmap = np.full((len(DTE_RANGE), len(VIX_RANGE)), np.nan)

    for i, dte in enumerate(DTE_RANGE):
        for j, vix in enumerate(VIX_RANGE):
            key = (dte, float(vix))
            if key in results:
                heatmap[i, j] = results[key]["kForSizing"]

    im = ax2.imshow(heatmap, aspect="auto", origin="lower",
                     cmap="RdYlGn_r",
                     extent=[VIX_RANGE[0]-0.5, VIX_RANGE[-1]+0.5, 0.5, len(DTE_RANGE)+0.5],
                     vmin=0.2, vmax=3.5)
    ax2.set_yticks(range(1, len(DTE_RANGE)+1))
    ax2.set_yticklabels(DTE_RANGE)
    ax2.set_xlabel("India VIX")
    ax2.set_ylabel("DTE")
    ax2.set_title("kForSizing Heatmap (DTE × VIX)")
    cbar = plt.colorbar(im, ax=ax2)
    cbar.set_label("kForSizing")

    # Add contour lines for key k values
    X, Y = np.meshgrid(VIX_RANGE, range(1, len(DTE_RANGE)+1))
    contours = ax2.contour(X, Y, heatmap, levels=[0.5, 1.0, 1.5, 2.0, 3.0],
                            colors="black", linewidths=0.8, alpha=0.6)
    ax2.clabel(contours, inline=True, fontsize=8, fmt="%.1f")

    # ── Chart 3: kForSizing vs VIX at different DTEs ──
    ax3 = axes[1, 0]
    dte_colors = {1: "#e74c3c", 2: "#e67e22", 3: "#f1c40f", 4: "#2ecc71", 5: "#3498db", 7: "#9b59b6"}

    for dte in [1, 2, 3, 5, 7]:
        ys = []
        xs = []
        for vix in VIX_RANGE:
            key = (dte, float(vix))
            if key in results:
                ys.append(results[key]["kForSizing"])
                xs.append(vix)
        if xs:
            ax3.plot(xs, ys, "-", color=dte_colors.get(dte, "gray"), linewidth=2,
                     label=f"DTE={dte}")

    # Mark static k values
    static_ks = {1: 1.0, 2: 0.70, 3: 0.55, 5: 0.45, 7: 0.40}
    for dte, sk in static_ks.items():
        ax3.axhline(y=sk, color=dte_colors.get(dte, "gray"), linestyle=":",
                     alpha=0.4, linewidth=1)
        ax3.text(VIX_RANGE[-1]+0.5, sk, f"static={sk}", fontsize=7,
                 color=dte_colors.get(dte, "gray"), va="center", alpha=0.6)

    ax3.set_xlabel("India VIX")
    ax3.set_ylabel("kForSizing")
    ax3.set_title("kForSizing vs VIX by DTE (dotted = static k)")
    ax3.legend(fontsize=9)
    ax3.grid(True, alpha=0.3)
    ax3.set_ylim(0, 4.0)

    # ── Chart 4: All 4 scenarios stacked bar at selected conditions ──
    ax4 = axes[1, 1]
    conditions = [
        ("1D\nVIX=12", 1, 12), ("1D\nVIX=20", 1, 20), ("1D\nVIX=28", 1, 28),
        ("2D\nVIX=12", 2, 12), ("2D\nVIX=20", 2, 20), ("2D\nVIX=28", 2, 28),
        ("4D\nVIX=12", 4, 12), ("4D\nVIX=20", 4, 20), ("4D\nVIX=28", 4, 28),
    ]
    scenario_colors_bar = {"kBase": "#2ecc71", "kStressMove": "#3498db",
                           "kStressVol": "#e67e22", "kCrash": "#e74c3c"}
    x_pos = np.arange(len(conditions))
    width = 0.18

    for i, scenario in enumerate(["kBase", "kStressMove", "kStressVol", "kCrash"]):
        vals = []
        for label, dte, vix in conditions:
            key = (dte, float(vix))
            if key in results:
                vals.append(results[key][scenario])
            else:
                vals.append(0)
        ax4.bar(x_pos + i * width, vals, width, label=scenario,
                color=scenario_colors_bar[scenario], alpha=0.85)

    ax4.set_xticks(x_pos + 1.5 * width)
    ax4.set_xticklabels([c[0] for c in conditions], fontsize=8)
    ax4.set_ylabel("k value")
    ax4.set_title("4 Scenarios Side-by-Side (selected conditions)")
    ax4.legend(fontsize=8)
    ax4.grid(True, alpha=0.2, axis="y")

    # ── Chart 5: Capital per lot (k × premium × lotSize) vs VIX ──
    ax5 = axes[2, 0]
    premiums = compute_premium_grid(spot, LOT_SIZE)

    for dte in [1, 2, 3, 5, 7]:
        ys = []
        xs = []
        for vix in VIX_RANGE:
            k_key = (dte, float(vix))
            if k_key in results and k_key in premiums:
                k = results[k_key]["kForSizing"]
                prem = premiums[k_key]
                capital_per_lot = k * prem * LOT_SIZE
                ys.append(capital_per_lot / 1000)  # in thousands
                xs.append(vix)
        if xs:
            ax5.plot(xs, ys, "-", color=dte_colors.get(dte, "gray"), linewidth=2,
                     label=f"DTE={dte}")

    ax5.set_xlabel("India VIX")
    ax5.set_ylabel("Capital per lot (₹ thousands)")
    ax5.set_title("Capital Required Per Lot = k × premium × lotSize\n(what actually drives sizing)")
    ax5.legend(fontsize=9)
    ax5.grid(True, alpha=0.3)

    # ── Chart 6: Premium vs VIX (explains why k drops at high VIX) ──
    ax6 = axes[2, 1]

    for dte in [1, 2, 3, 5, 7]:
        ys_prem = []
        ys_stresspnl = []
        xs = []
        for vix in VIX_RANGE:
            k_key = (dte, float(vix))
            if k_key in results and k_key in premiums:
                xs.append(float(vix))
                ys_prem.append(premiums[k_key])
                ys_stresspnl.append(abs(results[k_key]["pnlBreakdown"]["crashPnl"]))
        if xs:
            ax6.plot(xs, ys_prem, "-", color=dte_colors.get(dte, "gray"),
                     linewidth=2, label=f"Premium DTE={dte}")
            ax6.plot(xs, ys_stresspnl, "--", color=dte_colors.get(dte, "gray"),
                     linewidth=1.5, alpha=0.6, label=f"Crash P&L DTE={dte}")

    ax6.set_xlabel("India VIX")
    ax6.set_ylabel("₹ per unit")
    ax6.set_title("Premium (solid) vs Crash P&L (dashed)\n(premium grows faster → k drops at high VIX)")
    ax6.legend(fontsize=7, ncol=2)
    ax6.grid(True, alpha=0.3)

    plt.tight_layout(rect=[0, 0, 1, 0.97])

    out_path = "/Users/ekanshgowda/Documents/Code/Kite_API/k_scenarios_dashboard.png"
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    print(f"Dashboard saved to: {out_path}")
    plt.close()
    return out_path


if __name__ == "__main__":
    print("Computing k values across DTE × VIX grid...")
    results = compute_k_grid(SPOT_NIFTY, LOT_SIZE)
    print(f"Computed {len(results)} data points")

    plot_dashboard(results, SPOT_NIFTY)

    # Print a summary table
    print("\n" + "="*80)
    print("kForSizing summary (DTE rows × VIX columns)")
    print("="*80)
    vix_samples = [12, 16, 20, 24, 28, 32, 36, 40]
    header = f"{'DTE':>4}" + "".join(f"{'VIX='+str(v):>10}" for v in vix_samples)
    print(header)
    print("-" * len(header))
    for dte in DTE_RANGE:
        row = f"{dte:>4}"
        for vix in vix_samples:
            key = (dte, float(vix))
            if key in results:
                k = results[key]["kForSizing"]
                binding = results[key]["kBindingScenario"]
                tag = {"kBase": "B", "kStressMove": "M", "kStressVol": "V", "kCrash": "C"}[binding]
                row += f"{k:>8.2f}{tag:>2}"
            else:
                row += f"{'N/A':>10}"
        print(row)

    print("\nBinding legend: B=kBase, M=kStressMove, V=kStressVol, C=kCrash")
