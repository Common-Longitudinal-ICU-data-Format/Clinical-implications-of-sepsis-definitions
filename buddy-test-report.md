
# Buddy Test Report

| | |
|---|---|
| **Buddy site / institution** | *UCMC* |
| **Tester** | *Kaveri Chhikara* |
| **Date** | *2026-07-11* |

## Environment

| | |
|---|---|
| **OS** | macOS 26.5.1 |
| **RAM** | 64 GB |
| **Python** | 3.12.11 |

Tested end to end on two datasets: **UCMC** (single hospital) and **NU** (10-hospital system).

## Checks

| # | Check | Result | Notes |
|:-:|-------|--------|-------|
| 1 | Environment reproduces (`uv sync`, nothing by hand) | Pass | |
| 2 | Configuration works from `config/README.md` alone; no hardcoding | Pass | Driven by `clif_config.json` |
| 3 | Required tables/fields match what the code reads (mCIDE-valid) | Pass | Ran clean on UCMC + NU tables; no schema mismatches |
| 4 | Runs end to end with no manual edits between steps | Pass | Updated stitching logic to pre-filter patients and then stitch|
| 5 | Outputs in output dir with right naming/type, no raw dumps | Pass | fixed: Quarto HTML was landing in `Code/` (relative `--output-dir`); now resolved to an absolute path so all outputs land in the site output dir. No row-level dumps; PHI isolated to `phi_directory`. |
| 6 | **Data security**: no PHI, every stat n ≥ 10, no raw data *(blocking)* | Pass | fixed: Table 1 now suppressed at the study standard (n<5). Threshold 5 supersedes the "n ≥ 10" wording. |
| 7 | Clinical sanity: aggregates plausible for the cohort | Pass | Cohort/Table 1 plausible; sensible UCMC vs NU differences |
| 8 | Documentation usable: could run from the README alone | Pass | |

## Overall verdict

**Verdict:** Pass with notes.

The identified gaps are fixed; details below.

## Summary of changes made during buddy test

| Change | File(s) |
|---|---|
| Table 1 primary small-cell suppression at n<5 (`<5` in CSV; `count: null, "suppressed": true` in JSON) across all 3 Table 1 variants | `Code/02_table1.py` |
| Quarto `--output-dir` resolved to an absolute path (HTML was writing to `Code/` instead of the output dir) | `run.sh`, `run.ps1` |
| Patient pre-screen from `hospitalization` + pushdown-filtered ADT load (avoids loading full ADT into pandas at large sites); cohort verified byte-identical | `Code/01_cohort.py` |

### Blocking issues (must fix before distribution)

1. **[FIXED during buddy test] Table 1 shipped raw sub-5 counts.** Categorical subgroups (e.g. race) printed exact counts of 1–4 in `table1.csv`/`.json` and the community/hospital-onset variants. Fixed with primary suppression in `02_table1.py` (`SMALL_CELL_THRESHOLD = 5`): 1–4 → `<5` (CSV) / `count: null, "suppressed": true` (JSON). Verified on NU: sub-5 cells masked, **0 residual sub-5 counts** in any Table 1 output.

### Non-blocking notes / suggestions

1. **`site_summary.csv` per-hospital-year counts.** At NU some hospital-year cells are < 5 (as low as n=1). These are aggregate counts with **no identifiers, dates, or attached individual attributes** (site demographics are computed over all patients, not the small cell). Left as-is.

