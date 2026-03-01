# open-stock-data

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

Each CSV has columns: `code, name, exchange, listing_date`

### Bars (Published via GitHub Releases)

```
Release "bars-cn-2025" → daily_2025.parquet
Release "bars-hk-2025" → daily_2025.parquet
Release "bars-us-2025" → daily_2025.parquet
```

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
