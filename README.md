# Clinical Implications of Sepsis Definitions

**CLIF Version:** 2.1

Comparison of CDC Adult Sepsis Event (ASE) definitions with and without lactic acid as an organ dysfunction criterion across hospitalized adults.

**1st Author:** Kevin \| **Senior Author:** Will Parker

## Objective

Determine whether hospital-level lactate ordering intensity explains the gap between core (5-criterion) and extended (6-criterion, including lactate) CDC Adult Sepsis Event rates. The study compares ASE prevalence, patient characteristics, and outcomes across definitions using multi-site CLIF data.

## Important: ED + Floor Data Required

> **This study is NOT limited to ICU encounters.** Your CLIF tables must contain data from **Emergency Department (ED)**, **floor**, and **ICU** locations. The cohort is restricted to ED admissions and tracks organ dysfunction timing from ED arrival onward. Sites with ICU-only data cannot participate.

ADT records must include `location_category` values for `ed`, `icu`, and floor locations.

## Required CLIF Tables and Fields

### 1. `patient`

| Column               | Description                        |
|----------------------|------------------------------------|
| `patient_id`         | Unique patient identifier          |
| `race_category`      | Race category                      |
| `sex_category`       | Sex category                       |
| `ethnicity_category` | Ethnicity category                 |
| `death_dttm`         | Date/time of death (if applicable) |

### 2. `hospitalization`

| Column                    | Description                       |
|---------------------------|-----------------------------------|
| `patient_id`              | Unique patient identifier         |
| `hospitalization_id`      | Unique hospitalization identifier |
| `admission_dttm`          | Admission date/time               |
| `discharge_dttm`          | Discharge date/time               |
| `age_at_admission`        | Age at admission (years)          |
| `discharge_category`      | Discharge disposition             |
| `admission_type_category` | Admission type (e.g., ED)         |

### 3. `adt`

| Column               | Description                                         |
|------------------------------------|------------------------------------|
| `hospitalization_id` | Unique hospitalization identifier                   |
| `hospital_id`        | Hospital identifier                                 |
| `hospital_type`      | Hospital type (academic, community; excludes LTACH) |
| `location_category`  | Location category (`ed`, `icu`, floor)              |
| `location_type`      | Location subtype (e.g., MICU, SICU)                 |
| `in_dttm`            | Location entry date/time                            |
| `out_dttm`           | Location exit date/time                             |

### 4. `hospital_diagnosis`

| Column | Description |
|------------------------------------|------------------------------------|
| `hospitalization_id` | Unique hospitalization identifier |
| `diagnosis_code` | ICD diagnosis code (used for Charlson Comorbidity Index calculation) |

### 5. `labs`

| Column               | Description                       |
|----------------------|-----------------------------------|
| `hospitalization_id` | Unique hospitalization identifier |
| `lab_result_dttm`    | Lab result date/time              |
| `lab_category`       | Lab category                      |
| `lab_value_numeric`  | Numeric lab value                 |

**Required `lab_category` values:** `lactate`, `creatinine`, `bilirubin_total`, `platelet_count`

### 6. `vitals`

| Column               | Description                       |
|----------------------|-----------------------------------|
| `hospitalization_id` | Unique hospitalization identifier |
| `recorded_dttm`      | Vital sign recorded date/time     |
| `vital_category`     | Vital sign category               |
| `vital_value`        | Vital sign value                  |

Used for SOFA score computation.

### 7. `medication_admin_continuous`

| Column                | Description                       |
|-----------------------|-----------------------------------|
| `hospitalization_id`  | Unique hospitalization identifier |
| `admin_dttm`          | Administration date/time          |
| `med_name`            | Medication name                   |
| `med_category`        | Medication category               |
| `med_dose`            | Dose amount                       |
| `med_dose_unit`       | Dose unit                         |
| `mar_action_category` | MAR action category               |

**Required `med_category` values:** `vasoactives` (norepinephrine, epinephrine, phenylephrine, vasopressin, dopamine, etc.)

### 8. `respiratory_support`

| Column               | Description                       |
|----------------------|-----------------------------------|
| `hospitalization_id` | Unique hospitalization identifier |
| `recorded_dttm`      | Recorded date/time                |
| `device_category`    | Respiratory device category       |
| `mode_category`      | Ventilation mode                  |
| `fio2_set`           | FiO2 setting                      |
| `lpm_set`            | Liters per minute setting         |
| `peep_set`           | PEEP setting                      |

**Required `device_category` values:** `IMV`, `NIPPV`, `High Flow NC`

### 9. `microbiology_culture`

| Column               | Description                       |
|----------------------|-----------------------------------|
| `hospitalization_id` | Unique hospitalization identifier |
| `fluid_category`     | Specimen fluid category           |
| `collect_dttm`       | Collection date/time              |
| `result_dttm`        | Result date/time                  |
| `organism_category`  | Organism identified               |
| `method_category`    | Culture method                    |

**Required `fluid_category` values:** `blood_buffy` **Required `method_category` values:** `culture`

### 10. `crrt_therapy` (optional)

| Column               | Description                       |
|----------------------|-----------------------------------|
| `hospitalization_id` | Unique hospitalization identifier |
| `recorded_dttm`      | Recorded date/time                |

Used to identify CRRT receipt during hospitalization.

## Cohort Identification

The base cohort is defined by the following inclusion criteria:

1.  **Age** \>= 18 years at admission
2.  **Admission type**: ED admission only
3.  **Facility type**: Academic or community hospitals (excludes LTACH)
4.  **Time period**: Admitted and discharged between 2018-2024
5.  **ED location**: Must have a documented ED location in ADT records during the stay

From the base cohort, ASE status is computed using `clifpy.compute_ase()` under two definitions: - **ASE with lactate** (6 organ dysfunction criteria) - **ASE without lactate** (5 organ dysfunction criteria, excluding lactate)

Subgroups include community-onset ASE (within 48h of ED arrival), hospital-onset ASE (\>48h after ED arrival), and lactate-only ASE (meets criteria solely due to elevated lactate).

## Expected Results

Output files are written to the directory specified by `output_directory` in `clif_config.json`:

| Output | Description |
|------------------------------------|------------------------------------|
| `table1.csv` / `table1.json` | Demographics, comorbidities, acuity, life support, outcomes by ASE definition |
| `table1_community_onset.csv` | Table 1 restricted to community-onset ASE |
| `table1_hospital_onset.csv` | Table 1 restricted to hospital-onset ASE |
| `organ_dysfunction_breakdown.csv` | Organ dysfunction frequencies by definition |
| `lactate_orders_per_1000_patient_days.csv` | Hospital-level lactate ordering intensity |
| `plots/` | Sankey diagrams, temporal trends, QAD distributions (HTML + PNG) |

Protected intermediate files (`cohort_df.parquet`, `ase_results.parquet`, `analysis_dataset.parquet`) are written to the `phi_directory`.

## Instructions

### 1. Configure `clif_config.json`

Copy the template and fill in site-specific paths:

``` json
{
  "site_name": "your_site",
  "data_directory": "/path/to/clif/tables",
  "filetype": "parquet",
  "timezone": "US/Eastern",
  "output_directory": "./output_to_BOX",
  "phi_directory": "./phi"
}
```

-   `data_directory`: Path to your CLIF 2.1 parquet (or CSV) tables
-   `timezone`: Your site's local timezone
-   `output_directory`: Where de-identified results will be saved
-   `phi_directory`: Where PHI-containing intermediate files will be saved

### 2. Set up environment

This project uses [uv](https://docs.astral.sh/uv/) for Python dependency management. R and Quarto are required for script 04.

``` bash
uv sync
```

### 3. Run the pipeline

``` bash
bash run.sh
```

This executes all scripts sequentially and logs output to `logs/`.

## Pipeline Steps

| Step | Script | Description |
|------------------------|------------------------|------------------------|
| 1 | `Code/01_cohort.py` | Builds base cohort, computes ASE flags and organ dysfunction timestamps |
| 2 | `Code/02_table1.py` | Generates Table 1 with demographics, comorbidities, acuity, and outcomes |
| 3 | `Code/03_ase_visualizations.py` | Creates Sankey diagrams, temporal trends, and ordering intensity plots |
| 4 | `Code/04_ase_site_analysis.qmd` | Runs logistic regression modeling lactate ordering intensity vs. ASE rates |