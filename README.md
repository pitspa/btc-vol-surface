# BTC Implied Volatility Surface — Daily Snapshots

A proprietary rolling collection of Bitcoin option implied volatility surface data, sourced from the [Deribit](https://www.deribit.com/) public API.

## Purpose

This repository stores daily snapshots of the full BTC European option chain on Deribit. The data is intended for **Carr-Madan static replication** of variance-based signals (e.g., a BTC-specific VIX).

## Instrument Specification

Deribit BTC options are **European-style, cash-settled options on the Deribit BTC spot index**:

- **Exercise:** European — exercisable only at expiry, automatically.
- **Underlying:** The Deribit BTC Index, a composite spot price derived from multiple major exchanges (not a futures contract).
- **Settlement:** Cash-settled in BTC. At expiry, the delivery price is the 30-minute TWAP of the BTC spot index preceding 08:00 UTC.
- **Pricing model:** Deribit uses Black's model with a **per-expiry synthetic forward price** (derived via put-call parity or from the mark price of a matching listed future). This forward price — not the spot index — is used for IV computation and greeks. It varies by expiry (reflecting the basis/term structure) and is captured in the `forward_price` column.

## Data Structure

Each file in `surfaces/` is a self-contained daily CSV snapshot named `YYYY-MM-DD.csv`.

| Column | Description | Unit |
|---|---|---|
| `snapshot_ts` | UTC timestamp of the snapshot | ISO 8601 |
| `expiry` | Option expiration date | YYYY-MM-DD |
| `days_to_expiry` | Calendar days to expiration | int |
| `strike` | Strike price | USD |
| `option_type` | Call or Put | `C` / `P` |
| `bid` | Best bid price | BTC |
| `ask` | Best ask price | BTC |
| `mark_price` | Exchange mark price | BTC |
| `forward_price` | Synthetic forward price for this expiry (varies by expiry) | USD |
| `index_price` | BTC spot index price (same for all rows in a snapshot) | USD |
| `implied_vol` | Implied volatility (annualised) | decimal (e.g., 0.65 = 65%) |
| `delta` | Option delta | decimal |
| `gamma` | Option gamma | decimal |
| `vega` | Option vega | decimal |
| `open_interest` | Open interest | number of contracts |
| `volume_24h` | 24-hour trading volume | number of contracts |

**Note on `forward_price` vs `index_price`:** The `forward_price` is the per-expiry forward used by Deribit for pricing and IV (it differs across expiries due to the basis term structure). The `index_price` is the single BTC spot composite index at snapshot time. The difference `forward_price - index_price` represents the basis for that expiry.

## Rolling Window

The `surfaces/` directory maintains a **182-day (≈6 month) FIFO window**. Files older than 182 days are automatically removed by the cleanup script.

## Usage

### Install dependencies
```bash
pip install -r requirements.txt
```

### Collect today's snapshot
```bash
python scripts/collect.py
```

### Remove files older than the rolling window
```bash
python scripts/cleanup.py
```

## Configuration

Edit `config.json` to adjust parameters such as rolling window length or target currency.

## Data Source

All data is fetched from the **Deribit public REST API v2** (`https://www.deribit.com/api/v2/public/`). No API key or authentication is required.

## License

This repository and its contents are proprietary. Do not redistribute without permission.
