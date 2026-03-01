# Open Stock Data - Makefile
# Simplify common development commands

.PHONY: help install test lint lint-fix format check symbols bars backfill clean

# Default target shows help
help:
	@echo "Open Stock Data - Available Commands"
	@echo ""
	@echo "  Development:"
	@echo "    make install     Install/update dependencies (uv sync)"
	@echo "    make upgrade     Upgrade all dependencies (uv lock --upgrade)"
	@echo ""
	@echo "  Code Quality:"
	@echo "    make lint        Run linter (ruff check)"
	@echo "    make lint-fix    Auto-fix code issues (ruff check --fix)"
	@echo "    make format      Format code (ruff format)"
	@echo "    make check       Run all checks (lint + test)"
	@echo ""
	@echo "  Testing:"
	@echo "    make test        Run tests (pytest)"
	@echo "    make test-cov    Run tests with coverage report"
	@echo ""
	@echo "  Data Fetching:"
	@echo "    make symbols     Fetch all exchange symbols"
	@echo "    make symbols-cn  Fetch CN symbols (SSE + SZSE + BSE)"
	@echo "    make symbols-hk  Fetch HK symbols (HKEX)"
	@echo "    make symbols-us  Fetch US symbols (NASDAQ)"
	@echo "    make bars        Fetch all market bars"
	@echo "    make bars-cn     Fetch CN market bars"
	@echo "    make bars-hk     Fetch HK market bars"
	@echo "    make bars-us     Fetch US market bars"
	@echo "    make backfill    Backfill historical data"
	@echo ""
	@echo "  Maintenance:"
	@echo "    make clean       Clean cache files"
	@echo "    make clean-all   Clean all generated files (including venv)"

# ========== Development ==========

install:
	uv sync

upgrade:
	uv lock --upgrade

# ========== Code Quality ==========

lint:
	ruff check scripts tests

lint-fix:
	ruff check scripts tests --fix

format:
	ruff format scripts tests

check: lint test

# ========== Testing ==========

test:
	pytest

test-cov:
	pytest --cov=scripts --cov-report=term-missing --cov-report=html

# ========== Symbol Fetching ==========

symbols: symbols-cn symbols-hk symbols-us
	@echo "All symbols fetched successfully"

symbols-cn:
	@echo "Fetching CN symbols..."
	python scripts/fetch_symbols_sse.py
	python scripts/fetch_symbols_szse.py
	python scripts/fetch_symbols_bse.py

symbols-hk:
	@echo "Fetching HK symbols..."
	python scripts/fetch_symbols_hkex.py

symbols-us:
	@echo "Fetching US symbols..."
	python scripts/fetch_symbols_nasdaq.py

# ========== Bar Data Fetching ==========

bars: bars-cn bars-hk bars-us
	@echo "All bars fetched successfully"

bars-cn:
	@echo "Fetching CN bars..."
	python scripts/fetch_bars_cn.py

bars-hk:
	@echo "Fetching HK bars..."
	python scripts/fetch_bars_hk.py

bars-us:
	@echo "Fetching US bars..."
	python scripts/fetch_bars_us.py

# ========== Data Backfill ==========

backfill:
	python scripts/backfill.py

# ========== Cleanup ==========

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	rm -rf htmlcov/ .mypy_cache/ 2>/dev/null || true
	@echo "Cache files cleaned"

clean-all: clean
	@echo "Cleaning virtual environment..."
	rm -rf .venv/
	@echo "All generated files cleaned, run 'make install' to reinstall"
