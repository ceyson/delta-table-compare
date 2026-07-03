#!/usr/bin/env bash
# Sets up a local Python venv for testing the recon framework.
# Usage: source setup_env.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating venv at ${VENV_DIR}..."
    python3 -m venv "$VENV_DIR"
fi

source "${VENV_DIR}/bin/activate"

echo "Installing dependencies..."
pip install --upgrade pip setuptools wheel -q
pip install -r "${SCRIPT_DIR}/requirements-dev.txt" -q

echo ""
echo "Environment ready. To run tests:"
echo "  cd ${SCRIPT_DIR}"
echo "  pytest tests/ -v"
echo ""
echo "To run benchmarks:"
echo "  python -m benchmarks.bench_runner --engine both --profile all"
