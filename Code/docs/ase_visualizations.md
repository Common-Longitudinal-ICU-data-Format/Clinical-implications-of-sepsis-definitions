# ASE Organ Dysfunction Visualizations

## Overview

`Code/03_ase_visualizations.py` is a Marimo notebook that produces **33 output files** (18 plots + 15 CSVs) visualizing organ dysfunction patterns, temporal trends, and antimicrobial use in Adult Sepsis Events (ASE). All outputs are aggregated counts — no PHI is saved.

### Input data

| Source | Location | Description |
|--------|----------|-------------|
| `ase_results.parquet` | PHI directory | One row per hospitalization with ASE flags, organ-failure datetimes, QAD info, onset type |
| `cohort_df.parquet` | PHI directory | Cohort table; columns loaded: `hospitalization_id`, `hospital_id`, `hospital_type`, `admission_dttm` |
| CLIF Labs (lactate) | CLIF data directory | Loaded via `Labs.from_file()` with `lab_category=['lactate']`, filtered to cohort `hospitalization_id`s |

### Output directories

- **Plots:** `OUTPUT_DIR/plots/` (18 files — HTML and PNG)
- **Data:** `OUTPUT_DIR/data/` (15 CSV files)

### Global filters

- **Cohort:** age >= 18, ED admission, academic/community hospital (no LTACH)
- **Date range:** 2018–2024 (applied via `blood_culture_dttm` year for temporal plots, `ase_onset_w_lactate_dttm`/`ase_onset_wo_lactate_dttm` for onset type plots, `lab_result_dttm` year for lactate counts, `admission_dttm` year-month for ED summaries)

---

## Input Data & Filters

### `ase_results.parquet`

Loaded at line 71. Key columns used throughout:

| Column | Type | Description |
|--------|------|-------------|
| `hospitalization_id` | str | Join key |
| `sepsis` | int (0/1) | Meets ASE criteria with lactate as a valid organ dysfunction |
| `sepsis_wo_lactate` | int (0/1) | Meets ASE criteria excluding lactate criterion |
| `blood_culture_dttm` | datetime | Index blood-culture timestamp; used to derive `year_month` for most temporal plots |
| `ase_onset_w_lactate_dttm` | datetime | ASE onset datetime (with lactate definition); used to derive `year_month` for onset type (w/ lactate) plot |
| `ase_onset_wo_lactate_dttm` | datetime | ASE onset datetime (without lactate definition); used to derive `year_month` for onset type (w/o lactate) plot |
| `vasopressor_dttm` | datetime | First new vasopressor meeting ASE criteria |
| `imv_dttm` | datetime | First new IMV meeting ASE criteria |
| `aki_dttm` | datetime | Creatinine >= 2x baseline |
| `hyperbilirubinemia_dttm` | datetime | Bilirubin >= 2.0 and >= 2x baseline |
| `thrombocytopenia_dttm` | datetime | Platelet < 100 and <= 50% baseline |
| `lactate_dttm` | datetime | Lactate >= 2.0 mmol/L |
| `total_qad` | int | Qualifying Antimicrobial Days count |
| `type` | str | Onset type: `'community'` or `'hospital'` |
| `run_meds` | str | Comma-separated antimicrobial names from the QAD run |
| `qad_start_date` | date | Start of the QAD window |
| `qad_end_date` | date | End of the QAD window |

### `cohort_df.parquet`

Loaded at line 81. Columns: `hospitalization_id`, `hospital_id`, `hospital_type`, `admission_dttm`. Deduplicated on `hospitalization_id`.

### Lactate labs

Loaded at lines 894–905 via `Labs.from_file()`:
- Filters: `hospitalization_id` in cohort, `lab_category=['lactate']`
- Columns loaded: `hospitalization_id`, `lab_result_dttm`
- Joined with `cohort_df` on `hospitalization_id` for `hospital_id` and `hospital_type`

---

## Datetime Columns Reference

| Column | Definition | Source CLIF Table | Used In |
|--------|-----------|-------------------|---------|
| `blood_culture_dttm` | Index blood-culture timestamp | MicrobiologyCulture | Monthly cases, organ monthly, year derivation for temporal plots |
| `ase_onset_w_lactate_dttm` | ASE onset datetime (with lactate definition) | Derived (earliest qualifying organ dysfunction) | Monthly onset type (w/ lactate) |
| `ase_onset_wo_lactate_dttm` | ASE onset datetime (without lactate definition) | Derived (earliest qualifying organ dysfunction) | Monthly onset type (w/o lactate) |
| `vasopressor_dttm` | First new vasopressor within ASE window | MedicationAdminContinuous | Sankey, organ monthly |
| `imv_dttm` | First new IMV within ASE window | RespiratorySupport | Sankey, organ monthly |
| `aki_dttm` | Creatinine >= 2x baseline | Labs (creatinine) | Sankey, organ monthly |
| `hyperbilirubinemia_dttm` | Bilirubin >= 2.0 and >= 2x baseline | Labs (bilirubin_total) | Sankey, organ monthly |
| `thrombocytopenia_dttm` | Platelet < 100 and <= 50% baseline | Labs (platelet_count) | Sankey, organ monthly |
| `lactate_dttm` | Lactate >= 2.0 mmol/L | Labs (lactate) | Sankey (w/ lactate), organ monthly (w/ lactate) |
| `qad_start_date` / `qad_end_date` | QAD window date boundaries | Derived from MedicationAdmin | QAD lactate trend |
| `lab_result_dttm` | Lab result timestamp | Labs | Lactate counts, QAD lactate trend |
| `admission_dttm` | Hospital admission timestamp | Hospitalization | ED summary, year derivation |

---

## Hospitalization Filter Summary

| Output Group | Base Filter | Additional Filter |
|---|---|---|
| Sankey w/ lactate | `sepsis == 1` | — |
| Sankey w/o lactate | `sepsis_wo_lactate == 1` | — |
| QAD distribution | Both groups separately | — |
| Monthly ASE cases | All `ase_df` | Year 2018–2024 via `blood_culture_dttm` |
| Monthly organ dysfunctions | `sepsis == 1` / `sepsis_wo_lactate == 1` | Year 2018–2024 via `blood_culture_dttm` |
| Monthly onset type | `sepsis == 1` / `sepsis_wo_lactate == 1` | — (no year filter — all months from `ase_onset_w_lactate_dttm` / `ase_onset_wo_lactate_dttm`) |
| Top 20 QAD antimicrobials | `sepsis == 1` / `sepsis_wo_lactate == 1` | `run_meds` column exploded |
| Lactate lab counts | Full cohort | `lab_category=['lactate']`, year 2018–2024 via `lab_result_dttm` |
| QAD lactate trend | `sepsis == 1` / `sepsis_wo_lactate == 1` | Lactate labs where `lab_date` between `qad_start_date` and `qad_end_date`, year 2018–2024 |
| ED hospitalizations | Full `cohort_df` | Year-month derived from `admission_dttm` |

---

## Output File Catalog — Plots (18 files)

All plots are saved to `OUTPUT_DIR/plots/`. The save cell spans lines 1042–1346.

### 1. Sankey Plots (2 HTML)

#### `sankey_ase_w_lactate.html`
- **Format:** Interactive HTML (Plotly)
- **What it shows:** 6-level Sankey diagram showing the temporal sequence of organ dysfunctions for ASE patients (with lactate). Each level represents the Nth organ dysfunction to occur; flow widths represent patient counts.
- **Source:** `sankey_w_lactate` figure, built at lines 374–378 from `sequence_w_lactate`
- **Data object:** `prepare_organ_sequence(ase_w_lactate_df, ORGAN_COLS_WITH_LACTATE)` (lines 189–229)
- **Key columns:** All 6 organ datetime columns (`vasopressor_dttm`, `imv_dttm`, `aki_dttm`, `hyperbilirubinemia_dttm`, `thrombocytopenia_dttm`, `lactate_dttm`)
- **Filter:** `sepsis == 1`
- **Save:** line 1081

#### `sankey_ase_wo_lactate.html`
- **Format:** Interactive HTML (Plotly)
- **What it shows:** 5-level Sankey diagram for ASE patients without lactate criterion. Same structure as above but with 5 organ types.
- **Source:** `sankey_wo_lactate` figure, built at lines 424–428 from `sequence_wo_lactate`
- **Key columns:** 5 organ datetime columns (excludes `lactate_dttm`)
- **Filter:** `sepsis_wo_lactate == 1`
- **Save:** line 1085

### 2. QAD Distribution (1 HTML + 1 PNG)

#### `qad_distribution.html`
- **Format:** Interactive HTML (Plotly)
- **What it shows:** Side-by-side grouped bar chart comparing the distribution of QAD days (0–8) between ASE with-lactate and without-lactate groups.
- **Source:** `qad_distribution` figure, built at lines 513–517 via `create_qad_distribution()`
- **Key columns:** `total_qad`, `sepsis`, `sepsis_wo_lactate`
- **Filter:** `sepsis == 1` and `sepsis_wo_lactate == 1` (separately)
- **Save:** line 1090

#### `qad_distribution.png`
- **Format:** PNG (matplotlib, 150 DPI)
- **What it shows:** Same as the HTML version — static grouped bar chart.
- **Source:** Matplotlib recreation at lines 1094–1109
- **Save:** line 1108

### 3. Monthly ASE Cases (1 HTML + 1 PNG)

#### `yearly_cases.html`
- **Format:** Interactive HTML (Plotly)
- **What it shows:** Line chart of monthly ASE case counts comparing with-lactate vs without-lactate, filtered to 2018–2024.
- **Source:** `yearly_cases_fig` figure, built at lines 634–662
- **Key columns:** `blood_culture_dttm` (year_month derivation), `sepsis`, `sepsis_wo_lactate`
- **Filter:** All `ase_df`, year 2018–2024 from `blood_culture_dttm`
- **Save:** line 1114

#### `yearly_cases.png`
- **Format:** PNG (matplotlib, 150 DPI)
- **What it shows:** Same as the HTML version — static line chart. Excludes the "Total" row from the data.
- **Source:** Matplotlib recreation at lines 1118–1133
- **Save:** line 1131

### 4. Monthly Organ Dysfunctions (2 HTML + 2 PNG)

#### `monthly_organs_w_lactate.html`
- **Format:** Interactive HTML (Plotly)
- **What it shows:** Multi-line chart of monthly counts for each of 6 organ dysfunction types among ASE-with-lactate patients, 2018–2024.
- **Source:** `organ_monthly_w_fig` from `_build_organ_fig()` at lines 772–776
- **Key columns:** `blood_culture_dttm`, all 6 organ datetime columns
- **Filter:** `sepsis == 1`, year 2018–2024
- **Save:** line 1137

#### `monthly_organs_w_lactate.png`
- **Format:** PNG (matplotlib, 150 DPI)
- **What it shows:** Same as HTML — static multi-line chart. Excludes "Total" row.
- **Source:** Matplotlib recreation at lines 1149–1163
- **Save:** line 1161

#### `monthly_organs_wo_lactate.html`
- **Format:** Interactive HTML (Plotly)
- **What it shows:** Multi-line chart of monthly counts for 5 organ dysfunction types (no lactate) among ASE-without-lactate patients, 2018–2024.
- **Source:** `organ_monthly_wo_fig` from `_build_organ_fig()` at lines 779–783
- **Key columns:** `blood_culture_dttm`, 5 organ datetime columns (excludes `lactate_dttm`)
- **Filter:** `sepsis_wo_lactate == 1`, year 2018–2024
- **Save:** line 1167

#### `monthly_organs_wo_lactate.png`
- **Format:** PNG (matplotlib, 150 DPI)
- **What it shows:** Same as HTML — static multi-line chart. Excludes "Total" row.
- **Source:** Matplotlib recreation at lines 1171–1185
- **Save:** line 1183

### 5. Monthly Onset Type (2 HTML + 2 PNG)

#### `monthly_onset_w_lactate.html`
- **Format:** Interactive HTML (Plotly)
- **What it shows:** Two-line chart (Community vs Hospital onset) of monthly ASE-with-lactate case counts.
- **Source:** `yearly_onset_w_fig` from `_build_onset_fig()` at lines 857–861
- **Key columns:** `ase_onset_w_lactate_dttm`, `type` (`'community'`/`'hospital'`)
- **Filter:** `sepsis == 1`
- **Save:** line 1189

#### `monthly_onset_w_lactate.png`
- **Format:** PNG (matplotlib, 150 DPI)
- **What it shows:** Same as HTML — static two-line chart. Excludes "Total" row.
- **Source:** Matplotlib recreation at lines 1193–1210
- **Save:** line 1208

#### `monthly_onset_wo_lactate.html`
- **Format:** Interactive HTML (Plotly)
- **What it shows:** Two-line chart (Community vs Hospital onset) of monthly ASE-without-lactate case counts.
- **Source:** `yearly_onset_wo_fig` from `_build_onset_fig()` at lines 863–867
- **Key columns:** `ase_onset_wo_lactate_dttm`, `type`
- **Filter:** `sepsis_wo_lactate == 1`
- **Save:** line 1214

#### `monthly_onset_wo_lactate.png`
- **Format:** PNG (matplotlib, 150 DPI)
- **What it shows:** Same as HTML — static two-line chart. Excludes "Total" row.
- **Source:** Matplotlib recreation at lines 1218–1235
- **Save:** line 1233

### 6. QAD Lactate Trend (2 HTML + 2 PNG)

#### `qad_lactate_trend_w_lactate.html`
- **Format:** Interactive HTML (Plotly)
- **What it shows:** Monthly count of lactate lab orders that fall within QAD windows, stratified by hospital — for ASE-with-lactate episodes.
- **Source:** `qad_lactate_w_fig` from `_build_qad_lactate_trend()` at lines 1010–1015
- **Key columns:** `qad_start_date`, `qad_end_date`, `lab_result_dttm`, `hospital_id`
- **Filter:** `sepsis == 1`, lactate `lab_date` between `qad_start_date` and `qad_end_date`, year 2018–2024
- **Save:** line 1278

#### `qad_lactate_trend_w_lactate.png`
- **Format:** PNG (matplotlib, 150 DPI)
- **What it shows:** Same as HTML — static multi-line chart (one line per hospital).
- **Source:** Matplotlib recreation at lines 1282–1296
- **Save:** line 1294

#### `qad_lactate_trend_wo_lactate.html`
- **Format:** Interactive HTML (Plotly)
- **What it shows:** Monthly count of lactate lab orders within QAD windows, stratified by hospital — for ASE-without-lactate episodes.
- **Source:** `qad_lactate_wo_fig` from `_build_qad_lactate_trend()` at lines 1018–1023
- **Key columns:** Same as above
- **Filter:** `sepsis_wo_lactate == 1`, same QAD window and year filter
- **Save:** line 1304

#### `qad_lactate_trend_wo_lactate.png`
- **Format:** PNG (matplotlib, 150 DPI)
- **What it shows:** Same as HTML — static multi-line chart (one line per hospital).
- **Source:** Matplotlib recreation at lines 1308–1322
- **Save:** line 1320

---

## Output File Catalog — Data CSVs (15 files)

All CSVs are saved to `OUTPUT_DIR/data/`.

### 1. Sankey Transition Data (2 CSVs)

#### `sankey_ase_w_lactate_data.csv`
- **What it contains:** Aggregated transition counts between consecutive levels of the organ dysfunction sequence for ASE-with-lactate patients.
- **Columns:** `source_level` (int), `source_organ` (str), `target_level` (int), `target_organ` (str), `count` (int)
- **Row structure:** Each row = one (source organ at level N) -> (target organ at level N+1) transition. 5 transition pairs for 6 levels.
- **Source:** `sankey_w_lactate_data`, built at lines 381–391
- **Total row:** No
- **Save:** line 1241

#### `sankey_ase_wo_lactate_data.csv`
- **What it contains:** Same structure as above but for ASE-without-lactate patients. 4 transition pairs for 5 levels.
- **Columns:** `source_level` (int), `source_organ` (str), `target_level` (int), `target_organ` (str), `count` (int)
- **Row structure:** Each row = one organ-to-organ transition between consecutive levels.
- **Source:** `sankey_wo_lactate_data`, built at lines 431–441
- **Total row:** No
- **Save:** line 1245

### 2. QAD Distribution Data (1 CSV)

#### `qad_distribution_data.csv`
- **What it contains:** Count of patients at each QAD day value (0–8) for both ASE groups.
- **Columns:** `qad_days` (int, 0–8), `ase_with_lactate_count` (int), `ase_without_lactate_count` (int)
- **Row structure:** Each row = one QAD day value (9 rows total).
- **Source:** `qad_data_df`, built at lines 520–529
- **Total row:** No
- **Save:** line 1249

### 3. Monthly Cases Data (1 CSV)

#### `yearly_cases_data.csv`
- **What it contains:** Monthly ASE case counts for both definitions, with percentage columns.
- **Columns:** `year_month` (str, "YYYY-MM"), `sepsis` (int), `sepsis_wo_lactate` (int), `sepsis_pct` (float), `sepsis_wo_lactate_pct` (float)
- **Row structure:** Each row = one year-month period. Percentage columns are row-wise (each row's two groups sum to ~100%).
- **Source:** `yearly_cases`, built at lines 627–676
- **Total row:** Yes — final row with `year_month='Total'`
- **Save:** line 1253

### 4. Monthly Organ Dysfunction Data (2 CSVs)

#### `monthly_organs_w_lactate_data.csv`
- **What it contains:** Monthly counts of each organ dysfunction type among ASE-with-lactate patients, plus percentage columns.
- **Columns:** `year_month` (str), `Vasopressor` (int), `IMV` (int), `AKI` (int), `Hyperbilirubinemia` (int), `Thrombocytopenia` (int), `Lactate` (int), plus `{organ}_pct` (float) for each
- **Row structure:** Each row = one year-month. Percentage columns are row-wise (6 organs sum to ~100% per row).
- **Source:** `organ_monthly_w_pivot`, from `_build_organ_fig()` at lines 746–767
- **Total row:** Yes
- **Save:** line 1257

#### `monthly_organs_wo_lactate_data.csv`
- **What it contains:** Same structure but for ASE-without-lactate patients; 5 organ columns (no Lactate).
- **Columns:** `year_month` (str), `Vasopressor` (int), `IMV` (int), `AKI` (int), `Hyperbilirubinemia` (int), `Thrombocytopenia` (int), plus `{organ}_pct` (float) for each
- **Row structure:** Each row = one year-month. 5 organs sum to ~100% per row.
- **Source:** `organ_monthly_wo_pivot`, from `_build_organ_fig()` at lines 779–783
- **Total row:** Yes
- **Save:** line 1261

### 5. Monthly Onset Type Data (2 CSVs)

#### `monthly_onset_w_lactate_data.csv`
- **What it contains:** Monthly counts of community vs hospital onset for ASE-with-lactate, plus percentage columns. Months derived from `ase_onset_w_lactate_dttm`.
- **Columns:** `year_month` (str), `community` (float), `hospital` (float), `community_pct` (float), `hospital_pct` (float)
- **Row structure:** Each row = one year-month. `community` + `hospital` = total ASE cases that month; pct columns are row-wise.
- **Source:** `yearly_onset_w_data`, from `_build_onset_fig()` at lines 857–861
- **Total row:** Yes
- **Save:** line 1265

#### `monthly_onset_wo_lactate_data.csv`
- **What it contains:** Same structure for ASE-without-lactate. Months derived from `ase_onset_wo_lactate_dttm`.
- **Columns:** Same as above
- **Source:** `yearly_onset_wo_data`, from `_build_onset_fig()` at lines 863–867
- **Total row:** Yes
- **Save:** line 1269

### 6. Lactate Counts by Hospital (1 CSV)

#### `lactate_counts_by_hospital.csv`
- **What it contains:** Monthly lactate lab counts from the CLIF Labs table, grouped by hospital.
- **Columns:** `year_month` (str), `hospital_id` (str), `hospital_type` (str), `lactate_count` (int)
- **Row structure:** Each row = one (year-month, hospital) pair. Includes a "Total" row per hospital summing across all months.
- **Source:** `lactate_counts`, built at lines 918–929
- **Total row:** Yes — one "Total" row per hospital (with `year_month='Total'`)
- **Save:** line 1273

### 7. QAD Lactate Trend Data (2 CSVs)

#### `qad_lactate_trend_w_lactate_data.csv`
- **What it contains:** Monthly count of lactate lab orders occurring within QAD windows for ASE-with-lactate episodes, by hospital.
- **Columns:** `year_month` (str), `hospital_id` (str), `lactate_during_qad_count` (int)
- **Row structure:** Each row = one (year-month, hospital) pair.
- **Source:** `qad_lactate_w_data`, from `_build_qad_lactate_trend()` at lines 1010–1015
- **Total row:** No
- **Save:** line 1299

#### `qad_lactate_trend_wo_lactate_data.csv`
- **What it contains:** Same structure for ASE-without-lactate episodes.
- **Columns:** Same as above
- **Source:** `qad_lactate_wo_data`, from `_build_qad_lactate_trend()` at lines 1018–1023
- **Total row:** No
- **Save:** line 1325

### 8. ED Hospitalization Data (2 CSVs)

#### `monthly_ed_hospitalizations.csv`
- **What it contains:** Monthly ED hospitalization counts per hospital, derived from the full cohort.
- **Columns:** `month` (str, "YYYY-MM"), `hospital_id` (str), `hospital_type` (str), `n_hospitalizations` (int)
- **Row structure:** Each row = one (month, hospital) pair.
- **Source:** `monthly_hosp_counts`, built at lines 110–114
- **Total row:** No
- **Save:** line 1329

#### `monthly_ed_summary_stats.csv`
- **What it contains:** Summary statistics (mean, SD, median, IQR) of monthly ED hospitalization counts per hospital, plus an "Overall" row combining all hospitals.
- **Columns:** `hospital_id` (str), `hospital_type` (str), `mean` (float), `sd` (float), `median` (float), `q1` (float), `q3` (float), `n_months` (int)
- **Row structure:** Each row = one hospital. Final row has `hospital_id='Overall'`, `hospital_type='All'`.
- **Source:** `monthly_ed_summary`, built at lines 127–155
- **Total row:** Yes — "Overall" summary row
- **Save:** line 1333

### 9. Top 20 QAD Antimicrobials (2 CSVs)

#### `top20_qad_meds_w_lactate.csv`
- **What it contains:** The 20 most common antimicrobials contributing to QAD runs in ASE-with-lactate episodes, plus an "Other" row aggregating the rest.
- **Columns:** `antimicrobial` (str), `count` (int), `pct` (float)
- **Row structure:** Up to 21 rows (top 20 + "Other"). `pct` is each antimicrobial's share of total antimicrobial mentions.
- **Source:** `top20_meds_w_lactate`, built at lines 578–580 via `_top20_with_other()`
- **Total row:** No (but "Other" row aggregates the remainder)
- **Save:** line 1337

#### `top20_qad_meds_wo_lactate.csv`
- **What it contains:** Same structure for ASE-without-lactate episodes.
- **Columns:** Same as above
- **Source:** `top20_meds_wo_lactate`, built at lines 583–585
- **Save:** line 1341

---

## Summary Statistics Cell

**Lines 1358–1391** — Console output only (not saved to file).

This cell prints:
1. **Organ dysfunction counts and percentages** for both ASE groups — iterates over each organ datetime column and reports n (%) of non-null values
2. **Number of organ dysfunctions per patient** — value_counts of the sum of non-null organ datetime columns per patient, for both groups

These statistics are displayed during notebook execution but are not exported to any file.

---

## Key Notes

1. **No SITE_NAME prefix** on output filenames. Unlike some other scripts in this project, all filenames are static (e.g., `sankey_ase_w_lactate.html`, not `{SITE_NAME}_sankey_ase_w_lactate.html`).

2. **All temporal plots** derive `year_month` from `blood_culture_dttm` and filter to years 2018–2024, except:
   - Monthly onset type plots derive `year_month` from the ASE onset datetime (`ase_onset_w_lactate_dttm` for w/ lactate, `ase_onset_wo_lactate_dttm` for w/o lactate) and do **not** apply a year filter
   - Lactate lab counts use `lab_result_dttm` for the year filter
   - ED hospitalizations use `admission_dttm` for the year-month derivation

3. **Sankey `prepare_organ_sequence()`** (lines 189–229) orders organ dysfunctions by their datetime per patient, then pads with `"None"` to fill remaining slots. This creates a fixed-width sequence (6 levels with lactate, 5 without).

4. **CSV percentage columns are row-wise.** For multi-group CSVs (monthly cases, organs, onset), the percentage columns within each row sum to ~100%, showing the relative composition at that time point.

5. **All CSVs with temporal grouping** include a **"Total" summary row** (with `year_month='Total'`), except: Sankey transition CSVs, QAD distribution, QAD lactate trend, monthly ED hospitalizations, and top-20 antimicrobials.

6. **QAD lactate trend** merges lactate labs with QAD date windows via an inner join on `hospitalization_id`, then filters to rows where `lab_date` falls between `qad_start_date` and `qad_end_date` (inclusive on both sides).

7. **Top-20 antimicrobials** are derived by exploding the `run_meds` column (comma-separated) and counting individual medication mentions, not unique patients.

8. **PNG plots** are matplotlib recreations of the Plotly figures, saved at 150 DPI. The PNG versions exclude the "Total" row from temporal data to avoid plotting a non-temporal point.

9. **ED summary statistics** report the mean, SD, median, Q1, Q3 of monthly hospitalization counts per hospital. Each hospital's monthly counts are summarized across months, and the "Overall" row summarizes total monthly counts across all hospitals combined.
