#!/bin/bash
set -e

# Ambil directory tempat file .sh ini berada
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Config (mudah diubah)
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
LOG_DIR="$BASE_DIR/logs"
LOG_FILE="$LOG_DIR/run.log"

# Pastikan log directory ada
mkdir -p "$LOG_DIR"

# Run script
"$PYTHON_BIN" "$BASE_DIR/run.py" >> "$LOG_FILE" 2>&1