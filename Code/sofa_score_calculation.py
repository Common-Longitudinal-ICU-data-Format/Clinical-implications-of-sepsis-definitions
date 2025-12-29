"""
SOFA Score Calculation using CLIFpy

This script computes Sequential Organ Failure Assessment (SOFA) scores.
SOFA scores are used to track organ dysfunction in ICU patients.

The SOFA score evaluates 6 organ systems:
- Respiratory (PaO2/FiO2 ratio)
- Coagulation (platelet count)
- Liver (bilirubin)
- Cardiovascular (MAP and vasopressors)
- Central nervous system (GCS)
- Renal (creatinine)
"""

import pandas as pd
import warnings
from pathlib import Path

from clifpy.clif_orchestrator import ClifOrchestrator
from clifpy.utils.sofa import REQUIRED_SOFA_CATEGORIES_BY_TABLE

warnings.filterwarnings('ignore')

# =============================================================================
# Configuration
# =============================================================================
CONFIG_PATH = '../config/config.json' # UPDATE TO CORRECT CONFIG TODO
TIME_WINDOW_HOURS = 24  # Time window for SOFA calculation (e.g., first 24h) TODO PROJECT SPECIFIC 

# =============================================================================
# Initialize ClifOrchestrator
# =============================================================================
print("=" * 60)
print("SOFA Score Calculation")
print("=" * 60)

print("\nInitializing ClifOrchestrator...")
co = ClifOrchestrator(config_path=CONFIG_PATH)
print("✓ ClifOrchestrator initialized")

# =============================================================================
# Load Cohort Data- DEFINE YOUR COHORT- TODO
# =============================================================================
# Option 1: Load from hospitalization table (all patients)
print("\nLoading hospitalization data...")
co.load_table('hospitalization')
hosp_df = co.hospitalization.df

# Create cohort DataFrame with time windows
# Modify this section to match your cohort definition
cohort_df = pd.DataFrame({
    'hospitalization_id': hosp_df['hospitalization_id'],
    'start_time': pd.to_datetime(hosp_df['admission_dttm']),
    'end_time': pd.to_datetime(hosp_df['admission_dttm']) + pd.Timedelta(hours=TIME_WINDOW_HOURS)
})

# Option 2: If you have a specific cohort file, use this instead:
# cohort_df = pd.read_parquet('path/to/your/cohort.parquet')
# cohort_df['start_time'] = pd.to_datetime(cohort_df['your_start_column'])
# cohort_df['end_time'] = cohort_df['start_time'] + pd.Timedelta(hours=TIME_WINDOW_HOURS)

print(f"✓ Cohort prepared: {len(cohort_df):,} hospitalizations")

# Get list of hospitalization IDs for filtering
hosp_ids = cohort_df['hospitalization_id'].astype(str).unique().tolist()

# =============================================================================
# Load Required Tables for SOFA Computation
# =============================================================================
print("\nLoading tables for SOFA computation...")

# Load labs (creatinine, platelet count, PaO2, bilirubin)
co.load_table(
    'labs',
    filters={
        'hospitalization_id': hosp_ids,
        'lab_category': ['creatinine', 'platelet_count', 'po2_arterial', 'bilirubin_total']
    },
    columns=['hospitalization_id', 'lab_result_dttm', 'lab_category', 'lab_value_numeric']
)
print(f"  ✓ Labs loaded: {len(co.labs.df):,} records")

# Load vitals (MAP, SpO2, weight for dose calculations)
co.load_table(
    'vitals',
    filters={
        'hospitalization_id': hosp_ids,
        'vital_category': ['map', 'spo2', 'weight_kg', 'height_cm']
    },
    columns=['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value']
)
print(f"  ✓ Vitals loaded: {len(co.vitals.df):,} records")

# Load patient assessments (GCS for neurological SOFA)
co.load_table(
    'patient_assessments',
    filters={
        'hospitalization_id': hosp_ids,
        'assessment_category': ['gcs_total']
    },
    columns=['hospitalization_id', 'recorded_dttm', 'assessment_category', 'numerical_value']
)
print(f"  ✓ Patient assessments loaded: {len(co.patient_assessments.df):,} records")

# Load continuous medications (vasopressors for cardiovascular SOFA)
co.load_table(
    'medication_admin_continuous',
    filters={
        'hospitalization_id': hosp_ids,
        'med_category': ['norepinephrine', 'epinephrine', 'dopamine', 'dobutamine']
    }
)
print(f"  ✓ Medications loaded: {len(co.medication_admin_continuous.df):,} records")

# Load respiratory support (for FiO2 in respiratory SOFA)
co.load_table(
    'respiratory_support',
    filters={
        'hospitalization_id': hosp_ids
    },
    columns=['hospitalization_id', 'recorded_dttm', 'device_category', 'fio2_set']
)
print(f"  ✓ Respiratory support loaded: {len(co.respiratory_support.df):,} records")

print("✓ All SOFA tables loaded")

# =============================================================================
# Clean Medication Data
# =============================================================================
print("\nCleaning medication data...")
med_df = co.medication_admin_continuous.df.copy()
initial_med_count = len(med_df)

# Remove rows with null dose values
med_df = med_df[med_df['med_dose'].notna()]
med_df = med_df[med_df['med_dose_unit'].notna()]
med_df = med_df[~med_df['med_dose_unit'].astype(str).str.lower().isin(['nan', 'none', ''])]

# Update the table
co.medication_admin_continuous.df = med_df
print(f"✓ Removed null doses: {initial_med_count:,} → {len(med_df):,} records")

# =============================================================================
# Convert Medication Units for SOFA
# =============================================================================
print("\nConverting medication units for SOFA...")

# SOFA cardiovascular scoring requires vasopressor doses in mcg/kg/min
preferred_units = {
    'norepinephrine': 'mcg/kg/min',
    'epinephrine': 'mcg/kg/min',
    'dopamine': 'mcg/kg/min',
    'dobutamine': 'mcg/kg/min'
}

co.convert_dose_units_for_continuous_meds(
    preferred_units=preferred_units,
    override=True
)
print("✓ Medication units converted")

# =============================================================================
# Filter to Successful Conversions Only
# =============================================================================
print("\nFiltering medications to successful conversions...")
med_df_converted = co.medication_admin_continuous.df_converted.copy()
converted_initial_count = len(med_df_converted)

# Keep only rows with successful conversion status
med_df_success = med_df_converted[med_df_converted['_convert_status'] == 'success'].copy()

# Update the orchestrator's converted dataframe
co.medication_admin_continuous.df_converted = med_df_success

conversion_removed_count = converted_initial_count - len(med_df_success)
print(f"✓ Filtered: {converted_initial_count:,} → {len(med_df_success):,} records")
if converted_initial_count > 0:
    print(f"  Removed {conversion_removed_count:,} failed conversions ({conversion_removed_count/converted_initial_count*100:.1f}%)")

# =============================================================================
# Create Wide Dataset for SOFA
# =============================================================================
print("\nCreating wide dataset for SOFA...")

co.create_wide_dataset(
    category_filters=REQUIRED_SOFA_CATEGORIES_BY_TABLE,
    cohort_df=cohort_df,
    return_dataframe=True
)
print(f"✓ Wide dataset created: {co.wide_df.shape}")

# =============================================================================
# Add Missing Medication Columns
# =============================================================================
print("\nChecking for missing medication columns...")

required_med_cols = [
    'norepinephrine_mcg_kg_min',
    'epinephrine_mcg_kg_min',
    'dopamine_mcg_kg_min',
    'dobutamine_mcg_kg_min'
]

missing_cols = [col for col in required_med_cols if col not in co.wide_df.columns]

if missing_cols:
    for col in missing_cols:
        co.wide_df[col] = None
        print(f"  Added missing column: {col}")
    print(f"✓ Added {len(missing_cols)} missing medication columns")
else:
    print("✓ All medication columns present")

# =============================================================================
# Compute SOFA Scores
# =============================================================================
print("\nComputing SOFA scores...")

sofa_scores = co.compute_sofa_scores(
    wide_df=co.wide_df,
    id_name='hospitalization_id',
    fill_na_scores_with_zero=True,
    remove_outliers=True,
    create_new_wide_df=False
)

print(f"\n✓ SOFA scores computed: {sofa_scores.shape}")
print(f"  Mean SOFA: {sofa_scores['sofa_total'].mean():.2f}")
print(f"  Median SOFA: {sofa_scores['sofa_total'].median():.2f}")
print(f"  Range: {sofa_scores['sofa_total'].min():.0f} - {sofa_scores['sofa_total'].max():.0f}")

# =============================================================================
# Results
# =============================================================================
print("\n" + "=" * 60)
print("SOFA Score Results")
print("=" * 60)
print(sofa_scores.head(10))

# The sofa_scores DataFrame contains:
# - hospitalization_id
# - sofa_total (0-24)
# - Component scores: sofa_respiratory, sofa_coagulation, sofa_liver,
#                     sofa_cardiovascular, sofa_cns, sofa_renal

# =============================================================================
# Save Results (Optional)
# =============================================================================
output_path = Path('output')
output_path.mkdir(exist_ok=True)
sofa_output_file = output_path / 'sofa_scores.csv'
sofa_scores.to_csv(sofa_output_file, index=False)
print(f"\n✓ Results saved to: {sofa_output_file}")
