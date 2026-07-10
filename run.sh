#!/usr/bin/env bash
set -euo pipefail

export PYTHONUTF8=1

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

OUTPUT_DIR=$(uv run python -c 'import json; print(json.load(open("clif_config.json"))["output_directory"])')
mkdir -p "$OUTPUT_DIR"

echo "--- 04_ase_site_analysis_v9.qmd ---"
quarto render Code/04_ase_site_analysis_v9.qmd --output-dir "$OUTPUT_DIR"

echo "=== Run finished: $(date) ==="
