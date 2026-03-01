# open-stock-data

Daily-updated stock symbols and OHLCV bars for CN, HK, and US markets, powered by GitHub Actions.

## Project Overview

This project is an automated data pipeline that fetches daily stock market data from multiple exchanges and publishes it via GitHub Releases. It follows a **TDD (Test-Driven Development)** methodology and is designed to run autonomously via scheduled GitHub Actions workflows.

### Markets & Data Sources

| Market | Exchanges | Symbols Source | Bars Source |
|--------|-----------|----------------|-------------|
| CN | SSE, SZSE, BSE | Official exchange APIs | BaoStock (SSE/SZSE) + AKShare (BSE) |
| HK | HKEX | Official HKEX list | AKShare (东方财富 API) |
| US | NASDAQ, NYSE, AMEX | NASDAQ Screener API | AKShare (东方财富 API) |

### Data Storage

- **Symbols**: Git-tracked CSV files in `symbols/` directory
  - Schema: `code, region, name, exchange, type`
  - Files: `SSE.csv`, `SZSE.csv`, `BSE.csv`, `HKEX.csv`, `NASDAQ.csv`, `NYSE.csv`, `AMEX.csv`

- **Bars**: Published as Parquet files via GitHub Releases
  - Schema: `date, code, open, high, low, close, volume, amount, turnover`
  - All prices are **unadjusted** (raw)
  - Release tags: `data-cn-bars`, `data-hk-bars`, `data-us-bars`

## Technology Stack

- **Language**: Python 3.12+
- **Package Manager**: [uv](https://github.com/astral-sh/uv) (modern Python package manager)
- **Key Dependencies**:
  - `akshare` - Chinese financial data API
  - `baostock` - Alternative Chinese stock data source
  - `pandas` - Data manipulation
  - `pyarrow` - Parquet file I/O
  - `exchange-calendars` - Trading calendar for CN/HK/US markets
  - `requests` - HTTP client

### Development Tools

- **Linter/Formatter**: ruff (configured for Python 3.12, line length 120)
- **Testing**: pytest with coverage support
- **Build**: hatchling (PEP 517 build backend)

## Project Structure

```
.
├── scripts/                    # Main source code
│   ├── __init__.py
│   ├── fetch_symbols_*.py      # Exchange-specific symbol fetchers
│   │   ├── fetch_symbols_sse.py    # Shanghai Stock Exchange
│   │   ├── fetch_symbols_szse.py   # Shenzhen Stock Exchange
│   │   ├── fetch_symbols_bse.py    # Beijing Stock Exchange
│   │   ├── fetch_symbols_hkex.py   # Hong Kong Exchange
│   │   └── fetch_symbols_nasdaq.py # US markets (NASDAQ/NYSE/AMEX)
│   ├── fetch_bars_*.py         # Bar data fetchers
│   │   ├── fetch_bars_cn.py        # CN A-shares (BaoStock + AKShare)
│   │   ├── fetch_bars_hk.py        # HK stocks (AKShare)
│   │   └── fetch_bars_us.py        # US stocks (AKShare)
│   ├── backfill.py             # Historical data backfill CLI
│   └── utils/                  # Shared utilities
│       ├── __init__.py
│       ├── parquet_io.py       # Parquet read/write operations
│       ├── release_upload.py   # GitHub Releases API client
│       └── trading_calendar.py # Trading day utilities
├── tests/                      # Test suite (mirrors scripts structure)
│   ├── test_fetch_symbols_*.py
│   ├── test_fetch_bars_*.py
│   ├── test_parquet_io.py
│   ├── test_release_upload.py
│   ├── test_trading_calendar.py
│   └── test_backfill.py
├── symbols/                    # Symbol CSV files (git-tracked)
├── .github/workflows/          # CI/CD workflows
│   ├── ci.yml                  # Lint + test on push/PR
│   ├── update-symbols.yml      # Daily symbol update
│   ├── update-bars-cn.yml      # Daily CN bars update
│   ├── update-bars-hk.yml      # Daily HK bars update
│   └── update-bars-us.yml      # Daily US bars update
├── pyproject.toml              # Project configuration
├── Makefile                    # Common command shortcuts
└── README.md                   # Human documentation
```

## Development Setup

### Prerequisites

Install `uv` if not already installed:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install Dependencies

```bash
# Production dependencies
uv sync

# With dev dependencies (for development)
uv sync --group dev
```

### Verify Setup

```bash
# Run linting
uv run ruff check .

# Run tests
uv run pytest
```

## Build and Test Commands

### Using Make (Recommended)

```bash
# Development
make install      # Install/update dependencies
make upgrade      # Upgrade all dependencies

# Code Quality
make lint         # Run linter (ruff check)
make lint-fix     # Auto-fix code issues
make format       # Format code
make check        # Run all checks (lint + test)

# Testing
make test         # Run tests (pytest)
make test-cov     # Run tests with coverage report

# Data Fetching (local execution)
make symbols      # Fetch all exchange symbols
make symbols-cn   # Fetch CN symbols only
make symbols-hk   # Fetch HK symbols only
make symbols-us   # Fetch US symbols only
make bars         # Fetch all market bars
make bars-cn      # Fetch CN bars only
make bars-hk      # Fetch HK bars only
make bars-us      # Fetch US bars only

# Maintenance
make clean        # Clean cache files
make clean-all    # Clean everything including venv
```

### Using uv Directly

```bash
# Run scripts
uv run python -m scripts.fetch_symbols_sse
uv run python -m scripts.fetch_bars_cn

# Run backfill
uv run python -m scripts.backfill --market cn --start-year 2020 --end-year 2024 --local-only

# Run tests
uv run pytest
uv run pytest --cov=scripts --cov-report=term-missing
```

## Code Style Guidelines

### Python Style

- **Target**: Python 3.12+
- **Line length**: 120 characters
- **Import style**: Use `from __future__ import annotations` for forward references
- **Type hints**: Use type hints for all function signatures
- **Docstrings**: Use Google-style docstrings

### Linting Configuration (pyproject.toml)

```toml
[tool.ruff]
target-version = "py312"
line-length = 120

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors
    "W",   # pycodestyle warnings
    "F",   # pyflakes
    "I",   # isort
    "N",   # pep8-naming
    "UP",  # pyupgrade
    "B",   # flake8-bugbear
    "SIM", # flake8-simplify
    "TCH", # flake8-type-checking
    "RUF", # ruff-specific
]
```

### Code Patterns

1. **Function naming**: Use descriptive verb prefixes (`fetch_`, `save_`, `load_`, `validate_`)
2. **Error handling**: Use specific exceptions with descriptive messages
3. **Logging**: Use `logging.getLogger(__name__)` at module level
4. **Type checking**: Use `TYPE_CHECKING` guard for imports only needed for type hints
5. **Path handling**: Use `pathlib.Path` instead of string paths

### Example Module Structure

```python
"""Module docstring describing purpose and data sources."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import datetime

logger = logging.getLogger(__name__)


def fetch_data(...) -> pd.DataFrame:
    """Fetch data with descriptive docstring.
    
    Args:
        param: Description
        
    Returns:
        DataFrame with specific columns
        
    Raises:
        SpecificException: When condition occurs
    """
    ...
```

## Testing Instructions

### TDD Workflow

This project follows **Test-Driven Development**:

1. Define interface (function signatures + type hints + docstrings)
2. Write tests
3. Implement functionality

### Test Structure

- Tests are organized in classes by functionality
- Use descriptive test method names that explain the scenario
- Group related tests using class-based organization
- Use pytest fixtures (e.g., `tmp_path`) for temporary files

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_trading_calendar.py

# Run specific test class
uv run pytest tests/test_trading_calendar.py::TestIsTradingDay

# Run with coverage
uv run pytest --cov=scripts --cov-report=term-missing --cov-report=html
```

### Test Patterns

Tests use class-based organization with descriptive names:

```python
class TestIsTradingDay:
    """Tests for is_trading_day()."""
    
    def test_cn_normal_weekday_is_trading_day(self):
        """A regular Monday-Friday should be a trading day in CN."""
        assert is_trading_day("CN", datetime.date(2025, 12, 1)) is True
```

## GitHub Actions Workflows

### CI Workflow (ci.yml)

**Triggers**: Push/PR to main branch

**Jobs**:
1. Checkout code
2. Setup uv with caching
3. Install dependencies (`uv sync --group dev`)
4. Lint (`uv run ruff check .`)
5. Test (`uv run pytest --tb=short`)

### Update Symbols (update-symbols.yml)

**Schedule**: UTC 17:00 Mon-Fri (Beijing 01:00 next day)

**Steps**:
1. Fetch symbols from all 5 exchanges
2. Commit and push to repository if changes detected

### Update Bars Workflows

Each market has its own workflow:

| Workflow | Schedule | Description |
|----------|----------|-------------|
| update-bars-cn | UTC 09:00 Mon-Fri | After CN market close (15:00 Beijing) |
| update-bars-hk | UTC 09:00 Mon-Fri | After HK market close |
| update-bars-us | UTC 22:00 Mon-Fri | After US market close |

**Process**:
1. Determine target date (last trading day or manual input)
2. Skip if not a trading day
3. Load symbols from CSV files
4. Fetch bars from data sources
5. Download existing parquet from GitHub Releases (if any)
6. Append new data, deduplicate
7. Upload updated parquet to GitHub Releases

### Manual Workflow Triggers

All workflows support `workflow_dispatch` for manual execution with optional parameters (e.g., specific date for backfill).

## Deployment Process

### Data Publishing

Data is published via GitHub Releases, not git-tracked:

1. **Symbols**: Committed directly to git (CSV files in `symbols/`)
2. **Bars**: Uploaded as release assets (Parquet files)

### Release Tags

- `data-cn-bars` - CN A-share daily bars (`cn_bars.parquet`)
- `data-hk-bars` - HK equity daily bars (`hk_bars.parquet`)
- `data-us-bars` - US equity daily bars (`us_bars.parquet`)

### Required Permissions

GitHub Actions workflows need:
- `contents: write` - For committing symbols and creating releases
- `GITHUB_TOKEN` - For release asset uploads

## Security Considerations

### API Credentials

- No hardcoded credentials in source code
- GitHub token accessed via `secrets.GITHUB_TOKEN` in Actions
- For local backfill with upload, set `GITHUB_TOKEN` environment variable

### Data Validation

- All fetched data is validated against schemas before storage
- Parquet files use fixed schemas with type checking
- Symbol codes are validated with regex patterns

### Network Requests

- All HTTP requests have timeouts (30s default, 300s for downloads)
- API errors are logged but don't crash the pipeline
- Retry logic is handled by underlying libraries (akshare, requests)

### Local Development

- Virtual environment isolated in `.venv/`
- No production secrets in `.env` files (not used)
- Sensitive data should never be committed

## Utilities Reference

### Trading Calendar (`scripts/utils/trading_calendar.py`)

```python
from scripts.utils.trading_calendar import is_trading_day, last_trading_day

# Check if date is a trading day
is_trading_day("CN", datetime.date(2025, 1, 1))  # False (holiday)

# Get last trading day
last_trading_day("US")  # Returns most recent trading day
```

### Parquet I/O (`scripts/utils/parquet_io.py`)

```python
from scripts.utils.parquet_io import read_bars, write_bars, append_bars

# Read parquet
df = read_bars("path/to/file.parquet")

# Write parquet (creates parent dirs, validates schema)
write_bars("path/to/file.parquet", df)

# Append to existing (or create new)
append_bars("path/to/file.parquet", new_df)
```

### Release Upload (`scripts/utils/release_upload.py`)

```python
from scripts.utils.release_upload import (
    get_or_create_release,
    download_asset,
    upload_asset,
)

# Get or create release
release = get_or_create_release(repo, tag, token, name="Release Name")

# Download asset
download_asset(repo, tag, asset_name, dest_path, token)

# Upload asset (replaces existing with same name)
upload_asset(repo, tag, file_path, token)
```

## Common Tasks

### Backfill Historical Data

```bash
# Local only (no upload)
uv run python -m scripts.backfill --market cn --start-year 2020 --end-year 2024 --local-only

# With upload to GitHub Releases
export GITHUB_REPOSITORY=owner/repo
export GITHUB_TOKEN=ghp_xxx
uv run python -m scripts.backfill --market cn --start-year 2020
```

### Add a New Exchange

1. Create `scripts/fetch_symbols_<exchange>.py`
2. Create `tests/test_fetch_symbols_<exchange>.py`
3. Add to `Makefile` targets
4. Add to `.github/workflows/update-symbols.yml`
5. Update this documentation

### Update Dependencies

```bash
# Update lock file
uv lock --upgrade

# Sync to installed packages
uv sync
```
