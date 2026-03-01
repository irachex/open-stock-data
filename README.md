# open-stock-data

[![CI](https://github.com/irachex/open-stock-data/actions/workflows/ci.yml/badge.svg)](https://github.com/irachex/open-stock-data/actions/workflows/ci.yml)
[![Update Symbols](https://github.com/irachex/open-stock-data/actions/workflows/update-symbols.yml/badge.svg)](https://github.com/irachex/open-stock-data/actions/workflows/update-symbols.yml)
[![Update CN Bars](https://github.com/irachex/open-stock-data/actions/workflows/update-bars-cn.yml/badge.svg)](https://github.com/irachex/open-stock-data/actions/workflows/update-bars-cn.yml)
[![Update HK Bars](https://github.com/irachex/open-stock-data/actions/workflows/update-bars-hk.yml/badge.svg)](https://github.com/irachex/open-stock-data/actions/workflows/update-bars-hk.yml)
[![Update US Bars](https://github.com/irachex/open-stock-data/actions/workflows/update-bars-us.yml/badge.svg)](https://github.com/irachex/open-stock-data/actions/workflows/update-bars-us.yml)

Daily-updated stock symbols and OHLCV bars for CN, HK, and US markets, powered by GitHub Actions.

## Markets & Exchanges

| Market | Exchanges | Symbols Source | Bars Source |
|--------|-----------|---------------|-------------|
| CN | SSE, SZSE, BSE | Official exchange APIs | BaoStock (SSE/SZSE) + AKShare (BSE) |
| HK | HKEX | Official HKEX list | AKShare |
| US | NASDAQ, NYSE, AMEX | NASDAQ Screener API | AKShare |

## Data Structure

### Symbols (Git-tracked CSV files)

```
symbols/
  SSE.csv      # Shanghai Stock Exchange
  SZSE.csv     # Shenzhen Stock Exchange
  BSE.csv      # Beijing Stock Exchange
  HKEX.csv     # Hong Kong Exchange
  NASDAQ.csv   # NASDAQ
  NYSE.csv     # New York Stock Exchange
  AMEX.csv     # American Stock Exchange
```

Each CSV has columns: `code, region, name, exchange, type`

### Bars (Published via GitHub Releases)

| Release Tag | Asset | Description |
|-------------|-------|-------------|
| `data-cn-bars` | `cn_bars.parquet` | CN A-share daily bars |
| `data-hk-bars` | `hk_bars.parquet` | HK equity daily bars |
| `data-us-bars` | `us_bars.parquet` | US equity daily bars |

Parquet schema: `date, code, open, high, low, close, volume, amount, turnover`

All prices are **unadjusted** (raw). Users can compute adjusted prices as needed.

## Setup

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Install dev dependencies
uv sync --group dev
```

## Backfill

For initial historical data loading:

```bash
# Backfill CN bars (local only)
uv run python -m scripts.backfill --market cn --start-year 2020 --end-year 2024 --local-only

# Backfill and upload to GitHub Releases
export GITHUB_REPOSITORY=owner/repo
export GITHUB_TOKEN=ghp_xxx
uv run python -m scripts.backfill --market cn --start-year 2020

# Backfill HK / US
uv run python -m scripts.backfill --market hk --start-year 2022 --local-only
uv run python -m scripts.backfill --market us --start-year 2023 --local-only
```

## GitHub Actions

| Workflow | Schedule | Description |
|----------|----------|-------------|
| `update-symbols` | UTC 17:00 Mon-Fri | Fetch symbols from all exchanges |
| `update-bars-cn` | UTC 09:00 Mon-Fri | Fetch CN daily bars after market close |
| `update-bars-hk` | UTC 09:00 Mon-Fri | Fetch HK daily bars after market close |
| `update-bars-us` | UTC 22:00 Mon-Fri | Fetch US daily bars after market close |
| `ci` | On push/PR | Lint (ruff) + test (pytest) |

All workflows support `workflow_dispatch` for manual triggering.

## Development

This project follows **TDD** (Test-Driven Development):

1. Define interface (function signatures + type hints + docstrings)
2. Write tests
3. Implement functionality

Before every commit:

```bash
# Lint
uv run ruff check .

# Test
uv run pytest
```

Both must pass before committing.

## License

MIT
