# BTC Implied Volatility Surface — Daily Snapshots

A proprietary rolling collection of Bitcoin option implied volatility surface data, sourced from the [Deribit](https://www.deribit.com/) public API.

## Purpose

This repository stores daily snapshots of the full BTC European option chain on Deribit. The data is intended for **Carr-Madan static replication** of variance-based signals (e.g., a BTC-specific VIX).

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
| `underlying_price` | BTC index price at snapshot time | USD |
| `implied_vol` | Implied volatility (annualised) | decimal (e.g., 0.65 = 65%) |
| `delta` | Option delta | decimal |
| `gamma` | Option gamma | decimal |
| `vega` | Option vega | decimal |
| `open_interest` | Open interest | number of contracts |
| `volume_24h` | 24-hour trading volume | number of contracts |

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

All data is fetched from the **Deribit public REST API v2** (`https://www.deribit.com/api/v2/public/`). No API key or authentication is required. Options on Deribit are **European-style, cash-settled**.

## License

This repository and its contents are proprietary. Do not redistribute without permission.
