#!/usr/bin/env bash
set -e

LOG_FILE="logs/run_$(date +%Y%m%d_%H%M%S).log"
mkdir -p logs

exec > >(tee -a "$LOG_FILE") 2>&1

echo "=== Run started: $(date) ==="

echo "--- uv sync ---"
uv sync

echo "--- 01_cohort.py ---"
uv run python Code/01_cohort.py

echo "--- 02_table1.py ---"
uv run python Code/02_table1.py

echo "--- 03_ase_visualizations.py ---"
uv run python Code/03_ase_visualizations.py

echo "=== Run finished: $(date) ==="
