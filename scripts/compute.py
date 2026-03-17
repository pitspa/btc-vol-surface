"""
compute.py — Daily Signal Computation from Vol Surface Snapshots

Loads the most recent surface CSV, computes the Bakshi-Kapadia-Madan
static replication moments (n=2,3,4) for each eligible expiry, and
saves the results to the signals/ directory.

Usage (from repo root):
    python scripts/compute.py
"""

import sys
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from replication import static_replication_nth_moment

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config.json"

with open(CONFIG_PATH, "r") as f:
    CONFIG = json.load(f)

SURFACES_DIR = REPO_ROOT / CONFIG["surfaces_dir"]
SIGNALS_DIR = REPO_ROOT / CONFIG.get("signals_dir", "signals")

# Minimum number of OTM strikes (puts + calls combined) required
# for an expiry to be eligible for moment computation.
# Below this threshold the trapezoidal integral is too coarse,
# especially for higher moments (n=4) where tail weights matter.
MIN_OTM_STRIKES = CONFIG.get("min_otm_strikes", 15)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_latest_surface_file() -> Path:
    # Find the most recent CSV in surfaces/ by filename (YYYY-MM-DD.csv)
    csv_files = sorted(SURFACES_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {SURFACES_DIR}")
    return csv_files[-1]


def prepare_expiry_data(df_expiry, F, S0):
    # For a single expiry slice of the surface DataFrame:
    # - Deduplicate by strike: for K <= F keep the put, for K > F keep the call
    # - Convert BTC prices to USD using the forward price
    # - Sort by strike
    # - Return arrays: K, calls_usd, puts_usd

    # Separate calls and puts
    calls = df_expiry[df_expiry["option_type"] == "C"].copy()
    puts = df_expiry[df_expiry["option_type"] == "P"].copy()

    # Use mark_price as the option price; convert from BTC to USD
    calls["price_usd"] = calls["mark_price"] * F
    puts["price_usd"] = puts["mark_price"] * F

    # Index by strike for merging
    calls = calls.set_index("strike")["price_usd"].rename("call_usd")
    puts = puts.set_index("strike")["price_usd"].rename("put_usd")

    # Merge on strike — only keep strikes where both call and put exist
    # (needed for clean OTM selection at/near the forward)
    merged = pd.concat([calls, puts], axis=1).dropna()

    if merged.empty:
        return None, None, None

    merged = merged.sort_index()

    K = merged.index.values.astype(float)
    calls_usd = merged["call_usd"].values.astype(float)
    puts_usd = merged["put_usd"].values.astype(float)

    return K, calls_usd, puts_usd


def count_otm_strikes(K_array, F):
    # Count the number of OTM strikes: puts with K <= F, calls with K > F
    n_otm_puts = np.sum(K_array <= F)
    n_otm_calls = np.sum(K_array > F)
    return int(n_otm_puts + n_otm_calls)


# ---------------------------------------------------------------------------
# Main computation
# ---------------------------------------------------------------------------


def compute_signals(surface_path: Path) -> pd.DataFrame:
    # Load the surface CSV and compute moments for each eligible expiry

    df = pd.read_csv(surface_path)
    logger.info("Loaded surface: %s (%d rows)", surface_path.name, len(df))

    # Extract the snapshot-level spot index price (constant across rows)
    S0 = df["index_price"].iloc[0]
    snapshot_ts = df["snapshot_ts"].iloc[0]
    logger.info("Spot index: %.2f USD | Snapshot: %s", S0, snapshot_ts)

    # Group by expiry
    expiries = df.groupby("expiry")
    results = []

    for expiry_date, df_expiry in expiries:
        # Forward price for this expiry (constant within the expiry group)
        F = df_expiry["forward_price"].iloc[0]
        days_to_expiry = df_expiry["days_to_expiry"].iloc[0]

        # Skip expiries with non-positive time to maturity
        if days_to_expiry <= 0:
            logger.info("  %s: skipped (expired or same-day)", expiry_date)
            continue

        T = days_to_expiry / 365.0

        # Prepare strike/price arrays
        K, calls_usd, puts_usd = prepare_expiry_data(df_expiry, F, S0)

        if K is None:
            logger.info("  %s: skipped (no matched call/put pairs)", expiry_date)
            continue

        # Check OTM strike count
        n_otm = count_otm_strikes(K, F)
        if n_otm < MIN_OTM_STRIKES:
            logger.info("  %s: skipped (%d OTM strikes < %d minimum)",
                        expiry_date, n_otm, MIN_OTM_STRIKES)
            continue

        # Compute moments n=2,3,4
        try:
            mu2 = static_replication_nth_moment(2, K, calls_usd, puts_usd, F, S0, T)
            mu3 = static_replication_nth_moment(3, K, calls_usd, puts_usd, F, S0, T)
            mu4 = static_replication_nth_moment(4, K, calls_usd, puts_usd, F, S0, T)
        except Exception as exc:
            logger.error("  %s: computation failed — %s", expiry_date, exc)
            continue

        # SVIX = sqrt(mu2), annualised volatility units
        svix = np.sqrt(mu2) if mu2 > 0 else np.nan

        results.append({
            "snapshot_ts": snapshot_ts,
            "expiry": expiry_date,
            "days_to_expiry": days_to_expiry,
            "T_years": round(T, 6),
            "forward_price": F,
            "index_price": S0,
            "n_strikes": len(K),
            "n_otm_strikes": n_otm,
            "mu2_variance": mu2,
            "mu3_cubic": mu3,
            "mu4_quartic": mu4,
            "svix": svix,
        })

        logger.info("  %s: T=%.4f | %d strikes (%d OTM) | SVIX=%.4f | mu3=%.6f | mu4=%.6f",
                     expiry_date, T, len(K), n_otm, svix, mu3, mu4)

    if not results:
        logger.warning("No eligible expiries found.")
        return pd.DataFrame()

    return pd.DataFrame(results)


def save_signals(df: pd.DataFrame, surface_path: Path) -> Path:
    # Save signals with the same date-based filename as the surface
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
    date_str = surface_path.stem  # e.g. '2026-03-17'
    filepath = SIGNALS_DIR / f"{date_str}.csv"
    df.to_csv(filepath, index=False)
    logger.info("Saved signals: %s (%d expiries)", filepath.name, len(df))
    return filepath


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    logger.info("=== Signal Computation Started ===")

    try:
        surface_path = get_latest_surface_file()
    except FileNotFoundError as exc:
        logger.error("No surface data found: %s", exc)
        sys.exit(1)

    try:
        df_signals = compute_signals(surface_path)
    except Exception as exc:
        logger.error("Computation failed: %s", exc)
        sys.exit(1)

    if df_signals.empty:
        logger.error("No signals computed. Exiting with error.")
        sys.exit(1)

    save_signals(df_signals, surface_path)
    logger.info("=== Computation Complete ===")


if __name__ == "__main__":
    main()
