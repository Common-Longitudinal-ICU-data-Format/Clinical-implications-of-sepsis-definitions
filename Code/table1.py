import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # Table 1: Demographics and Outcomes by Sepsis Definition

    Compares demographics and outcomes across:
    1. Presumed infection
    2. ASE with lactate
    3. ASE without lactate
    """)
    return


@app.cell
def _():
    import json
    import pandas as pd
    from pathlib import Path
    from clifpy.tables import (
        Adt, CrrtTherapy, HospitalDiagnosis, Labs,
        MedicationAdminContinuous, MicrobiologyCulture,
        RespiratorySupport, Vitals
    )
    return (
        Adt,
        CrrtTherapy,
        HospitalDiagnosis,
        Labs,
        MedicationAdminContinuous,
        MicrobiologyCulture,
        Path,
        RespiratorySupport,
        Vitals,
        json,
        pd,
    )


@app.cell
def _(Path, json):
    # Load configuration
    config_path = Path("clif_config.json")
    config = json.loads(config_path.read_text())

    DATA_DIR = config["data_directory"]
    FILETYPE = config["filetype"]
    TIMEZONE = config["timezone"]
    OUTPUT_DIR = Path(config["output_directory"])
    SITE_NAME = config["site_name"]

    print(f"Site: {SITE_NAME}")
    print(f"Data directory: {DATA_DIR}")
    return DATA_DIR, FILETYPE, OUTPUT_DIR, SITE_NAME, TIMEZONE


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 1: Load Cohort and ASE Results
    """)
    return


@app.cell
def _(OUTPUT_DIR, SITE_NAME, pd):
    # Load cohort and ASE results from notebook 1
    cohort_df = pd.read_parquet(OUTPUT_DIR / f"{SITE_NAME}_cohort_df.parquet")
    ase_df = pd.read_parquet(OUTPUT_DIR / f"{SITE_NAME}_ase_results.parquet")

    # Get hosp_ids from base cohort
    hosp_ids = cohort_df['hospitalization_id'].astype(str).unique().tolist()

    print(f"Cohort: {len(cohort_df):,} hospitalizations")
    print(f"ASE results: {len(ase_df):,} episodes")
    return ase_df, cohort_df, hosp_ids


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 2: Load Additional CLIF Tables
    """)
    return


@app.cell
def _(Adt, DATA_DIR, FILETYPE, TIMEZONE, hosp_ids):
    adt = Adt.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE,
        filters={'hospitalization_id': hosp_ids}
    )
    print(f"ADT: {len(adt.df):,} rows")
    return (adt,)


@app.cell
def _(CrrtTherapy, DATA_DIR, FILETYPE, TIMEZONE, hosp_ids):
    try:
        crrt = CrrtTherapy.from_file(
            data_directory=DATA_DIR,
            filetype=FILETYPE,
            timezone=TIMEZONE,
            filters={'hospitalization_id': hosp_ids}
        )
        print(f"CRRT: {len(crrt.df):,} rows")
    except Exception as crrt_err:
        print(f"CRRT table not available: {crrt_err}")
        crrt = None
    return (crrt,)


@app.cell
def _(DATA_DIR, FILETYPE, RespiratorySupport, TIMEZONE, hosp_ids):
    resp = RespiratorySupport.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE,
        filters={
            'hospitalization_id': hosp_ids,
            'device_category': ['IMV', 'NIPPV', 'High Flow NC']
        }
    )
    print(f"Respiratory support: {len(resp.df):,} rows")
    return (resp,)


@app.cell
def _(DATA_DIR, FILETYPE, MedicationAdminContinuous, TIMEZONE, hosp_ids):
    meds_cont = MedicationAdminContinuous.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE,
        filters={
            'hospitalization_id': hosp_ids,
            'med_group': ['vasoactives']
        }
    )
    print(f"Continuous meds (vasoactives): {len(meds_cont.df):,} rows")
    return (meds_cont,)


@app.cell
def _(DATA_DIR, FILETYPE, MicrobiologyCulture, TIMEZONE, hosp_ids):
    micro = MicrobiologyCulture.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE,
        filters={
            'hospitalization_id': hosp_ids,
            'fluid_category': ['blood_buffy']
        }
    )
    print(f"Microbiology (blood cultures): {len(micro.df):,} rows")
    return (micro,)


@app.cell
def _(DATA_DIR, FILETYPE, HospitalDiagnosis, TIMEZONE, hosp_ids):
    dx = HospitalDiagnosis.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE,
        filters={'hospitalization_id': hosp_ids}
    )
    print(f"Hospital diagnosis: {len(dx.df):,} rows")
    return (dx,)


@app.cell
def _(DATA_DIR, FILETYPE, TIMEZONE, Vitals, hosp_ids):
    # Load Vitals to get first vital time per hospitalization
    vitals_for_time = Vitals.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE,
        filters={'hospitalization_id': hosp_ids},
        columns=['hospitalization_id', 'recorded_dttm']
    )
    # Get first vital per hospitalization
    first_vital_df = vitals_for_time.df.groupby('hospitalization_id')['recorded_dttm'].min().reset_index()
    first_vital_df.columns = ['hospitalization_id', 'first_vital_dttm']
    print(f"First vital times computed: {len(first_vital_df):,} hospitalizations")
    return (first_vital_df,)


@app.cell
def _(DATA_DIR, FILETYPE, Labs, TIMEZONE, hosp_ids):
    # Load Labs for lactate counts
    labs_lactate = Labs.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE,
        filters={
            'hospitalization_id': hosp_ids,
            'lab_category': ['lactate']
        },
        columns=['hospitalization_id', 'lab_result_dttm', 'lab_category', 'lab_value_numeric']
    )
    print(f"Lactate labs loaded: {len(labs_lactate.df):,} records")
    return (labs_lactate,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 3: Compute Derived Variables (Vectorized)
    """)
    return


@app.cell
def _(adt, hosp_ids, pd):
    # Compute ICU indicators - VECTORIZED (no for loops)
    adt_raw = adt.df.copy()
    icu_stays = adt_raw[adt_raw['location_category'] == 'icu'].copy()

    # Start with base DataFrame
    icu_df = pd.DataFrame({'hospitalization_id': hosp_ids})

    # Any ICU
    hosp_with_any_icu = set(icu_stays['hospitalization_id'].unique())
    icu_df['had_icu'] = icu_df['hospitalization_id'].isin(hosp_with_any_icu)

    # ICU types - vectorized
    icu_types_list = ['cardiac_icu', 'neuro_icu', 'surgical_icu', 'medical_icu', 'general_icu']
    for_icu_type = {t: set(icu_stays[icu_stays['location_type'] == t]['hospitalization_id'].unique()) for t in icu_types_list}
    icu_df['icu_cardiac_icu'] = icu_df['hospitalization_id'].isin(for_icu_type.get('cardiac_icu', set()))
    icu_df['icu_neuro_icu'] = icu_df['hospitalization_id'].isin(for_icu_type.get('neuro_icu', set()))
    icu_df['icu_surgical_icu'] = icu_df['hospitalization_id'].isin(for_icu_type.get('surgical_icu', set()))
    icu_df['icu_medical_icu'] = icu_df['hospitalization_id'].isin(for_icu_type.get('medical_icu', set()))
    icu_df['icu_general_icu'] = icu_df['hospitalization_id'].isin(for_icu_type.get('general_icu', set()))

    # First ICU time
    first_icu = icu_stays.groupby('hospitalization_id')['in_dttm'].min().reset_index()
    first_icu.columns = ['hospitalization_id', 'first_icu_dttm']
    icu_df = pd.merge(icu_df, first_icu, on='hospitalization_id', how='left')

    # ICU LOS
    icu_stays['icu_duration'] = (pd.to_datetime(icu_stays['out_dttm']) - pd.to_datetime(icu_stays['in_dttm'])).dt.total_seconds() / 86400
    icu_los_agg = icu_stays.groupby('hospitalization_id')['icu_duration'].sum().reset_index()
    icu_los_agg.columns = ['hospitalization_id', 'icu_los_days']
    icu_df = pd.merge(icu_df, icu_los_agg, on='hospitalization_id', how='left')

    print(f"ICU indicators computed: {icu_df['had_icu'].sum():,} patients with ICU stay")
    return (icu_df,)


@app.cell
def _(crrt, hosp_ids, pd):
    # Compute CRRT indicators - VECTORIZED
    crrt_df = pd.DataFrame({'hospitalization_id': hosp_ids})
    if crrt is not None and len(crrt.df) > 0:
        hosp_with_crrt = set(crrt.df['hospitalization_id'].unique())
        crrt_df['had_crrt'] = crrt_df['hospitalization_id'].isin(hosp_with_crrt)
    else:
        crrt_df['had_crrt'] = False
    print(f"CRRT indicators computed: {crrt_df['had_crrt'].sum():,} patients with CRRT")
    return (crrt_df,)


@app.cell
def _(hosp_ids, pd, resp):
    # Compute respiratory support indicators - VECTORIZED
    resp_raw = resp.df.copy()
    resp_df = pd.DataFrame({'hospitalization_id': hosp_ids})

    hosp_imv = set(resp_raw[resp_raw['device_category'] == 'IMV']['hospitalization_id'].unique())
    hosp_nippv = set(resp_raw[resp_raw['device_category'] == 'NIPPV']['hospitalization_id'].unique())
    hosp_hfno = set(resp_raw[resp_raw['device_category'] == 'High Flow NC']['hospitalization_id'].unique())

    resp_df['had_imv'] = resp_df['hospitalization_id'].isin(hosp_imv)
    resp_df['had_nippv'] = resp_df['hospitalization_id'].isin(hosp_nippv)
    resp_df['had_hfno'] = resp_df['hospitalization_id'].isin(hosp_hfno)

    print(f"Respiratory: IMV={resp_df['had_imv'].sum():,}, NIPPV={resp_df['had_nippv'].sum():,}, HFNO={resp_df['had_hfno'].sum():,}")
    return (resp_df,)


@app.cell
def _(hosp_ids, meds_cont, pd):
    # Compute vasopressor indicators - VECTORIZED
    vaso_df = pd.DataFrame({'hospitalization_id': hosp_ids})
    hosp_vaso = set(meds_cont.df['hospitalization_id'].unique())
    vaso_df['had_vasopressor'] = vaso_df['hospitalization_id'].isin(hosp_vaso)
    print(f"Vasopressor indicators: {vaso_df['had_vasopressor'].sum():,} patients")
    return (vaso_df,)


@app.cell
def _(hosp_ids, micro, pd):
    # Compute microbiology indicators - VECTORIZED
    micro_raw = micro.df.copy()
    positive_cultures = micro_raw[
        (micro_raw['organism_category'].notna()) &
        (micro_raw['organism_category'] != 'no_growth')
    ]

    # Count per hospitalization
    culture_counts = positive_cultures.groupby('hospitalization_id').size().reset_index(name='positive_culture_count')

    micro_df = pd.DataFrame({'hospitalization_id': hosp_ids})
    micro_df = pd.merge(micro_df, culture_counts, on='hospitalization_id', how='left')
    micro_df['positive_culture_count'] = micro_df['positive_culture_count'].fillna(0).astype(int)

    # Top 20 organisms
    top_organisms = positive_cultures['organism_category'].value_counts().head(20)
    print(f"Microbiology computed: {(micro_df['positive_culture_count'] > 0).sum():,} patients with positive cultures")
    return micro_df, top_organisms


@app.cell
def _(dx, pd):
    # Compute CCI - returns NEW DataFrame
    try:
        from clifpy.utils import calculate_cci
        cci_result = calculate_cci(dx, hierarchy=True)
        cci_df = cci_result[['hospitalization_id', 'cci_score']].copy()
        print(f"CCI computed: mean={cci_df['cci_score'].mean():.1f}")
    except Exception as cci_err:
        print(f"CCI calculation failed: {cci_err}")
        cci_df = pd.DataFrame({'hospitalization_id': dx.df['hospitalization_id'].unique(), 'cci_score': 0})
    return (cci_df,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 4: Compute SOFA Scores
    """)
    return


@app.cell
def _(Path, cohort_df, hosp_ids, json, pd):
    # Compute SOFA scores using ClifOrchestrator
    from clifpy.clif_orchestrator import ClifOrchestrator
    from clifpy.utils.sofa import REQUIRED_SOFA_CATEGORIES_BY_TABLE

    sofa_cohort = pd.DataFrame({
        'hospitalization_id': cohort_df['hospitalization_id'],
        'start_time': pd.to_datetime(cohort_df['admission_dttm']),
        'end_time': pd.to_datetime(cohort_df['discharge_dttm'])
    })

    # Load config inline (avoid reusing 'f')
    sofa_cfg = json.loads(Path("clif_config.json").read_text())

    co = ClifOrchestrator(
        data_directory=sofa_cfg["data_directory"],
        filetype=sofa_cfg["filetype"],
        timezone=sofa_cfg["timezone"]
    )

    print("Loading tables for SOFA...")

    co.load_table('labs', filters={
        'hospitalization_id': hosp_ids,
        'lab_category': ['creatinine', 'platelet_count', 'po2_arterial', 'bilirubin_total']
    }, columns=['hospitalization_id', 'lab_result_dttm', 'lab_category', 'lab_value_numeric'])

    co.load_table('vitals', filters={
        'hospitalization_id': hosp_ids,
        'vital_category': ['map', 'spo2', 'weight_kg', 'height_cm']
    }, columns=['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value'])

    co.load_table('patient_assessments', filters={
        'hospitalization_id': hosp_ids,
        'assessment_category': ['gcs_total']
    }, columns=['hospitalization_id', 'recorded_dttm', 'assessment_category', 'numerical_value'])

    co.load_table('medication_admin_continuous', filters={
        'hospitalization_id': hosp_ids,
        'med_category': ['norepinephrine', 'epinephrine', 'dopamine', 'dobutamine']
    })

    co.load_table('respiratory_support', filters={'hospitalization_id': hosp_ids},
                  columns=['hospitalization_id', 'recorded_dttm', 'device_category', 'fio2_set'])

    # Clean and convert medication data
    if len(co.medication_admin_continuous.df) > 0:
        sofa_med_df = co.medication_admin_continuous.df.copy()
        sofa_med_df = sofa_med_df[sofa_med_df['med_dose'].notna()]
        sofa_med_df = sofa_med_df[sofa_med_df['med_dose_unit'].notna()]
        sofa_med_df = sofa_med_df[~sofa_med_df['med_dose_unit'].astype(str).str.lower().isin(['nan', 'none', ''])]
        co.medication_admin_continuous.df = sofa_med_df

        sofa_preferred_units = {
            'norepinephrine': 'mcg/kg/min', 'epinephrine': 'mcg/kg/min',
            'dopamine': 'mcg/kg/min', 'dobutamine': 'mcg/kg/min'
        }
        co.convert_dose_units_for_continuous_meds(preferred_units=sofa_preferred_units, override=True)

        if hasattr(co.medication_admin_continuous, 'df_converted'):
            sofa_med_success = co.medication_admin_continuous.df_converted[
                co.medication_admin_continuous.df_converted['_convert_status'] == 'success'
            ].copy()
            co.medication_admin_continuous.df_converted = sofa_med_success

    # Create wide dataset
    co.create_wide_dataset(
        category_filters=REQUIRED_SOFA_CATEGORIES_BY_TABLE,
        cohort_df=sofa_cohort,
        return_dataframe=True
    )

    # Add missing med columns - use list comprehension instead of for loop
    sofa_med_cols = ['norepinephrine_mcg_kg_min', 'epinephrine_mcg_kg_min', 'dopamine_mcg_kg_min', 'dobutamine_mcg_kg_min']
    missing_sofa_cols = [c for c in sofa_med_cols if c not in co.wide_df.columns]
    if missing_sofa_cols:
        co.wide_df = co.wide_df.assign(**{c: None for c in missing_sofa_cols})

    # Compute SOFA
    sofa_scores = co.compute_sofa_scores(
        wide_df=co.wide_df,
        id_name='hospitalization_id',
        fill_na_scores_with_zero=True,
        remove_outliers=True,
        create_new_wide_df=False
    )

    # Get max SOFA per hospitalization
    sofa_df = sofa_scores.groupby('hospitalization_id')['sofa_total'].max().reset_index()
    sofa_df.columns = ['hospitalization_id', 'max_sofa']

    print(f"SOFA computed: mean={sofa_df['max_sofa'].mean():.1f}, median={sofa_df['max_sofa'].median():.1f}")
    return (sofa_df,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 5: Assemble Analysis DataFrame
    """)
    return


@app.cell
def _(
    ase_df,
    cci_df,
    cohort_df,
    crrt_df,
    first_vital_df,
    icu_df,
    micro_df,
    pd,
    resp_df,
    sofa_df,
    vaso_df,
):
    # SINGLE ASSEMBLY POINT - merge all data into analysis_df
    analysis_df = pd.merge(
        cohort_df,
        ase_df[['hospitalization_id', 'presumed_infection', 'sepsis', 'sepsis_wo_lactate', 'type',
                'blood_culture_dttm', 'aki_dttm', 'vasopressor_dttm', 'hyperbilirubinemia_dttm',
                'thrombocytopenia_dttm', 'lactate_dttm', 'imv_dttm',
                'ase_onset_w_lactate_dttm', 'ase_onset_wo_lactate_dttm',
                'presumed_infection_onset_dttm']],
        on='hospitalization_id',
        how='left'
    )

    # Fill NaN
    analysis_df['presumed_infection'] = analysis_df['presumed_infection'].fillna(0).astype(int)
    analysis_df['sepsis'] = analysis_df['sepsis'].fillna(0).astype(int)
    analysis_df['sepsis_wo_lactate'] = analysis_df['sepsis_wo_lactate'].fillna(0).astype(int)

    # Create group indicators
    # ASE with lactate: Meets ASE using 6 organ dysfunction criteria (lactate CAN count)
    # ASE without lactate: Meets ASE using only 5 organ dysfunction criteria (lactate CANNOT count)
    # Note: These groups are NOT mutually exclusive - a patient can be in both
    analysis_df['group_presumed_infection'] = analysis_df['presumed_infection'] == 1
    analysis_df['group_ase_w_lactate'] = analysis_df['sepsis'] == 1
    analysis_df['group_ase_wo_lactate'] = analysis_df['sepsis_wo_lactate'] == 1

    # Merge all derived DataFrames
    analysis_df = analysis_df.merge(icu_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(crrt_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(resp_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(vaso_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(micro_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(sofa_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(cci_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(first_vital_df, on='hospitalization_id', how='left')

    # Compute time to first organ failure (hours from first vital)
    organ_failure_cols = ['aki_dttm', 'vasopressor_dttm', 'hyperbilirubinemia_dttm',
                          'thrombocytopenia_dttm', 'lactate_dttm', 'imv_dttm']
    # Convert to datetime if needed
    for col in organ_failure_cols:
        if col in analysis_df.columns:
            analysis_df[col] = pd.to_datetime(analysis_df[col], errors='coerce')
    analysis_df['first_vital_dttm'] = pd.to_datetime(analysis_df['first_vital_dttm'], errors='coerce')

    # Get earliest organ failure datetime per patient
    analysis_df['first_organ_failure_dttm'] = analysis_df[organ_failure_cols].min(axis=1)

    # Calculate hours from first vital to first organ failure
    analysis_df['time_to_organ_failure_hours'] = (
        (analysis_df['first_organ_failure_dttm'] - analysis_df['first_vital_dttm']).dt.total_seconds() / 3600
    )

    # Fill NaN values
    analysis_df['max_sofa'] = analysis_df['max_sofa'].fillna(0)
    analysis_df['cci_score'] = analysis_df['cci_score'].fillna(0)
    analysis_df['positive_culture_count'] = analysis_df['positive_culture_count'].fillna(0).astype(int)

    # Organ failure indicators
    analysis_df['had_aki'] = analysis_df['aki_dttm'].notna()
    analysis_df['had_hyperbili'] = analysis_df['hyperbilirubinemia_dttm'].notna()
    analysis_df['had_thrombocytopenia'] = analysis_df['thrombocytopenia_dttm'].notna()
    analysis_df['had_elevated_lactate'] = analysis_df['lactate_dttm'].notna()

    # LOS calculations
    analysis_df['admission_dttm'] = pd.to_datetime(analysis_df['admission_dttm'])
    analysis_df['discharge_dttm'] = pd.to_datetime(analysis_df['discharge_dttm'])
    analysis_df['hospital_los_days'] = (
        (analysis_df['discharge_dttm'] - analysis_df['admission_dttm']).dt.total_seconds() / 86400
    )

    # Time to blood culture (hours from admission)
    analysis_df['time_to_bc_hours'] = (
        (pd.to_datetime(analysis_df['blood_culture_dttm']) - analysis_df['admission_dttm']).dt.total_seconds() / 3600
    )

    # In-hospital death
    analysis_df['in_hospital_death'] = (
        (analysis_df['discharge_category'].str.lower().str.contains('expired', na=False)) |
        (analysis_df['death_dttm'].notna() &
         (pd.to_datetime(analysis_df['death_dttm']) <= analysis_df['discharge_dttm']))
    )

    print(f"Analysis dataset assembled: {len(analysis_df):,} hospitalizations")
    print(f"Presumed infection: {analysis_df['group_presumed_infection'].sum():,}")
    print(f"ASE with lactate: {analysis_df['group_ase_w_lactate'].sum():,}")
    print(f"ASE without lactate: {analysis_df['group_ase_wo_lactate'].sum():,}")
    return (analysis_df,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 6: Create Table 1
    """)
    return


@app.cell
def _():
    # Helper functions for summarizing variables
    def summarize_continuous(df, col):
        """Summarize continuous variable as mean (SD)"""
        return f"{df[col].mean():.1f} ({df[col].std():.1f})"

    def summarize_binary(df, col):
        """Summarize binary variable as n (%)"""
        n = df[col].sum() if isinstance(col, str) else col.sum()
        total = len(df)
        return "0 (0.0%)" if total == 0 else f"{int(n)} ({100*n/total:.1f}%)"

    def summarize_median_iqr(df, col):
        """Summarize continuous variable as median (IQR)"""
        vals = df[col].dropna()
        if len(vals) == 0:
            return "N/A"
        med = vals.median()
        q25 = vals.quantile(0.25)
        q75 = vals.quantile(0.75)
        return f"{med:.1f} ({q25:.1f}-{q75:.1f})"
    return summarize_binary, summarize_continuous, summarize_median_iqr


@app.cell
def _(
    analysis_df,
    pd,
    summarize_binary,
    summarize_continuous,
    summarize_median_iqr,
):
    # Create Table 1 - using dict comprehensions instead of for loops
    groups = {
        'Total': analysis_df,
        'Presumed Infection': analysis_df[analysis_df['group_presumed_infection']],
        'ASE with Lactate': analysis_df[analysis_df['group_ase_w_lactate']],
        'ASE without Lactate': analysis_df[analysis_df['group_ase_wo_lactate']]
    }

    table1_rows = []

    # N
    table1_rows.append({'Variable': 'N', **{nm: str(len(df)) for nm, df in groups.items()}})

    # Demographics
    table1_rows.append({'Variable': '--- Demographics ---', **{nm: '' for nm in groups}})
    table1_rows.append({'Variable': 'Age, mean (SD)', **{nm: summarize_continuous(df, 'age_at_admission') for nm, df in groups.items()}})

    # Sex - Male
    table1_rows.append({'Variable': 'Sex - Male, n (%)', **{nm: summarize_binary(df, df['sex_category'].str.lower() == 'male') for nm, df in groups.items()}})
    # Sex - Female
    table1_rows.append({'Variable': 'Sex - Female, n (%)', **{nm: summarize_binary(df, df['sex_category'].str.lower() == 'female') for nm, df in groups.items()}})

    # Race categories - get top 5
    table1_rows.append({'Variable': '--- Race ---', **{nm: '' for nm in groups}})
    race_cats = analysis_df['race_category'].dropna().unique()[:5].tolist()
    table1_rows.extend([
        {'Variable': f'Race - {rc}, n (%)', **{nm: summarize_binary(df, df['race_category'] == rc) for nm, df in groups.items()}}
        for rc in race_cats
    ])

    # Comorbidities
    table1_rows.append({'Variable': '--- Comorbidities ---', **{nm: '' for nm in groups}})
    table1_rows.append({'Variable': 'CCI, mean (SD)', **{nm: summarize_continuous(df, 'cci_score') for nm, df in groups.items()}})

    # Acuity
    table1_rows.append({'Variable': '--- Acuity ---', **{nm: '' for nm in groups}})
    table1_rows.append({'Variable': 'Max SOFA, mean (SD)', **{nm: summarize_continuous(df, 'max_sofa') for nm, df in groups.items()}})

    # ICU
    table1_rows.append({'Variable': '--- ICU ---', **{nm: '' for nm in groups}})
    table1_rows.append({'Variable': 'Any ICU, n (%)', **{nm: summarize_binary(df, 'had_icu') for nm, df in groups.items()}})

    icu_type_cols = ['icu_cardiac_icu', 'icu_neuro_icu', 'icu_surgical_icu', 'icu_medical_icu']
    table1_rows.extend([
        {'Variable': f'{col}, n (%)', **{nm: summarize_binary(df, col) for nm, df in groups.items()}}
        for col in icu_type_cols
    ])

    # Life Support
    table1_rows.append({'Variable': '--- Life Support ---', **{nm: '' for nm in groups}})
    life_support = [('had_crrt', 'CRRT'), ('had_imv', 'IMV'), ('had_nippv', 'NIPPV'), ('had_hfno', 'HFNO'), ('had_vasopressor', 'Vasopressor')]
    table1_rows.extend([
        {'Variable': f'{lbl}, n (%)', **{nm: summarize_binary(df, vr) for nm, df in groups.items()}}
        for vr, lbl in life_support
    ])

    # Organ Failure
    table1_rows.append({'Variable': '--- Organ Failure (CDC) ---', **{nm: '' for nm in groups}})
    organ_failure = [('had_aki', 'AKI'), ('had_hyperbili', 'Hyperbilirubinemia'), ('had_thrombocytopenia', 'Thrombocytopenia'), ('had_elevated_lactate', 'Elevated Lactate')]
    table1_rows.extend([
        {'Variable': f'{lbl}, n (%)', **{nm: summarize_binary(df, vr) for nm, df in groups.items()}}
        for vr, lbl in organ_failure
    ])
    # Time to first organ failure
    table1_rows.append({
        'Variable': 'Time to first organ failure (hours), median (IQR)',
        **{nm: summarize_median_iqr(df, 'time_to_organ_failure_hours') for nm, df in groups.items()}
    })

    # Outcomes
    table1_rows.append({'Variable': '--- Outcomes ---', **{nm: '' for nm in groups}})
    table1_rows.append({'Variable': 'Hospital LOS, mean (SD)', **{nm: summarize_continuous(df, 'hospital_los_days') for nm, df in groups.items()}})
    table1_rows.append({
        'Variable': 'ICU LOS, mean (SD)',
        **{nm: summarize_continuous(df[df['had_icu']], 'icu_los_days') if df['had_icu'].sum() > 0 else 'N/A' for nm, df in groups.items()}
    })
    table1_rows.append({'Variable': 'In-hospital death, n (%)', **{nm: summarize_binary(df, 'in_hospital_death') for nm, df in groups.items()}})

    # Microbiology
    table1_rows.append({'Variable': '--- Microbiology ---', **{nm: '' for nm in groups}})
    table1_rows.append({'Variable': 'Positive blood cultures, mean (SD)', **{nm: summarize_continuous(df, 'positive_culture_count') for nm, df in groups.items()}})
    table1_rows.append({
        'Variable': 'Time to blood culture (hours), median (IQR)',
        **{nm: summarize_median_iqr(df, 'time_to_bc_hours') for nm, df in groups.items()}
    })

    table1 = pd.DataFrame(table1_rows)
    print("Table 1 created")
    return (table1,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 7: Save Outputs
    """)
    return


@app.cell
def _(OUTPUT_DIR, SITE_NAME, table1, top_organisms):
    # Save main Table 1
    table1_path = OUTPUT_DIR / f"{SITE_NAME}_table1.csv"
    table1.to_csv(table1_path, index=False)
    print(f"Table 1 saved to: {table1_path}")

    # Save top organisms
    organisms_path = OUTPUT_DIR / f"{SITE_NAME}_top20_organisms.csv"
    top_organisms.to_csv(organisms_path)
    print(f"Top 20 organisms saved to: {organisms_path}")
    return


@app.cell
def _(table1):
    table1
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 7a: Site-Level Summary for Multi-Site Analysis
    """)
    return


@app.cell
def _(OUTPUT_DIR, SITE_NAME, analysis_df, pd):
    # Create site-level summary grouped by year, health_system, hospital_id, hospital_type
    # This output is designed for aggregation across multiple sites

    # Create a copy to avoid modifying analysis_df
    site_df = analysis_df.copy()

    # Add derived columns
    site_df['year'] = pd.to_datetime(site_df['admission_dttm']).dt.year
    site_df['health_system'] = SITE_NAME
    site_df['is_female'] = site_df['sex_category'].str.lower() == 'female'

    # Group by dimensions
    group_cols = ['year', 'health_system', 'hospital_id', 'hospital_type']

    # Aggregate function
    def compute_site_summary(grp):
        return pd.Series({
            # Counts by onset type
            'n_presumed_infection_community': ((grp['group_presumed_infection']) & (grp['type'] == 'community')).sum(),
            'n_presumed_infection_hospital': ((grp['group_presumed_infection']) & (grp['type'] == 'hospital')).sum(),
            'n_presumed_infection_all': grp['group_presumed_infection'].sum(),
            'n_ASE_nolactic_community': ((grp['group_ase_wo_lactate']) & (grp['type'] == 'community')).sum(),
            'n_ASE_nolactic_hospital': ((grp['group_ase_wo_lactate']) & (grp['type'] == 'hospital')).sum(),
            'n_ASE_nolactic_all': grp['group_ase_wo_lactate'].sum(),
            'n_ASE_lactic_community': ((grp['group_ase_w_lactate']) & (grp['type'] == 'community')).sum(),
            'n_ASE_lactic_hospital': ((grp['group_ase_w_lactate']) & (grp['type'] == 'hospital')).sum(),
            'n_ASE_lactic_all': grp['group_ase_w_lactate'].sum(),
            'n_BSE_community': None,  # BSE not implemented
            'n_BSE_hospital': None,
            'n_BSE_all': None,
            # Statistics
            'total_n_patients': len(grp),
            'female_allpatients_average': grp['is_female'].mean(),
            'female_allpatients_sd': grp['is_female'].std(),
            'age_allpatients_average': grp['age_at_admission'].mean(),
            'age_allpatients_sd': grp['age_at_admission'].std(),
            'CCI_average_allpatients': grp['cci_score'].mean(),
            'CCI_sd_allpatients': grp['cci_score'].std(),
            'SOFA_average_allpatients': grp['max_sofa'].mean(),
            'SOFA_sd_allpatients': grp['max_sofa'].std(),
        })

    site_summary = site_df.groupby(group_cols, group_keys=False).apply(compute_site_summary).reset_index()

    # Save
    site_summary_path = OUTPUT_DIR / f"{SITE_NAME}_site_summary.csv"
    site_summary.to_csv(site_summary_path, index=False)
    print(f"Site summary saved to: {site_summary_path}")
    print(f"  Rows: {len(site_summary)} (unique year/hospital combinations)")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 7b: Lactate Counts Before Criteria
    """)
    return


@app.cell
def _(
    OUTPUT_DIR,
    SITE_NAME,
    analysis_df,
    labs_lactate,
    pd,
    summarize_median_iqr,
):
    # Compute lactate counts before each criteria was met
    lactate_df = labs_lactate.df.copy()
    lactate_df['lab_result_dttm'] = pd.to_datetime(lactate_df['lab_result_dttm'], errors='coerce')

    # Vectorized computation using merge and filter
    # Get criteria times from analysis_df
    criteria_times = analysis_df[['hospitalization_id', 'presumed_infection_onset_dttm',
                                   'ase_onset_w_lactate_dttm', 'ase_onset_wo_lactate_dttm']].copy()

    # Convert to datetime
    criteria_times['presumed_infection_onset_dttm'] = pd.to_datetime(criteria_times['presumed_infection_onset_dttm'], errors='coerce')
    criteria_times['ase_onset_w_lactate_dttm'] = pd.to_datetime(criteria_times['ase_onset_w_lactate_dttm'], errors='coerce')
    criteria_times['ase_onset_wo_lactate_dttm'] = pd.to_datetime(criteria_times['ase_onset_wo_lactate_dttm'], errors='coerce')

    # Merge lactate times with criteria times
    lactate_with_criteria = pd.merge(lactate_df, criteria_times, on='hospitalization_id', how='left')

    # Count lactates before each criteria using vectorized operations
    lactate_with_criteria['before_presumed_infection'] = (
        lactate_with_criteria['lab_result_dttm'] < lactate_with_criteria['presumed_infection_onset_dttm']
    )
    lactate_with_criteria['before_ase_w_lactate'] = (
        lactate_with_criteria['lab_result_dttm'] < lactate_with_criteria['ase_onset_w_lactate_dttm']
    )
    lactate_with_criteria['before_ase_wo_lactate'] = (
        lactate_with_criteria['lab_result_dttm'] < lactate_with_criteria['ase_onset_wo_lactate_dttm']
    )

    # Aggregate counts per hospitalization
    lactate_counts = lactate_with_criteria.groupby('hospitalization_id').agg(
        lactates_before_presumed_infection=('before_presumed_infection', 'sum'),
        lactates_before_ase_w_lactate=('before_ase_w_lactate', 'sum'),
        lactates_before_ase_wo_lactate=('before_ase_wo_lactate', 'sum')
    ).reset_index()

    # Merge back to analysis_df to get group info
    lactate_counts_with_groups = pd.merge(
        lactate_counts,
        analysis_df[['hospitalization_id', 'group_presumed_infection', 'group_ase_w_lactate', 'group_ase_wo_lactate']],
        on='hospitalization_id',
        how='left'
    )

    # Create summary by group
    lactate_summary_rows = []

    # Presumed infection group
    pi_subset = lactate_counts_with_groups[lactate_counts_with_groups['group_presumed_infection']]
    lactate_summary_rows.append({
        'Group': 'Presumed Infection',
        'Criteria': 'Before Presumed Infection',
        'N': len(pi_subset),
        'Lactates before criteria, median (IQR)': summarize_median_iqr(pi_subset, 'lactates_before_presumed_infection')
    })

    # ASE with lactate group
    ase_w_subset = lactate_counts_with_groups[lactate_counts_with_groups['group_ase_w_lactate']]
    lactate_summary_rows.append({
        'Group': 'ASE with Lactate',
        'Criteria': 'Before ASE with Lactate onset',
        'N': len(ase_w_subset),
        'Lactates before criteria, median (IQR)': summarize_median_iqr(ase_w_subset, 'lactates_before_ase_w_lactate')
    })

    # ASE without lactate group
    ase_wo_subset = lactate_counts_with_groups[lactate_counts_with_groups['group_ase_wo_lactate']]
    lactate_summary_rows.append({
        'Group': 'ASE without Lactate',
        'Criteria': 'Before ASE without Lactate onset',
        'N': len(ase_wo_subset),
        'Lactates before criteria, median (IQR)': summarize_median_iqr(ase_wo_subset, 'lactates_before_ase_wo_lactate')
    })

    lactate_summary = pd.DataFrame(lactate_summary_rows)

    # Save lactate summary
    lactate_summary_path = OUTPUT_DIR / f"{SITE_NAME}_lactate_counts_summary.csv"
    lactate_summary.to_csv(lactate_summary_path, index=False)
    print(f"Lactate summary saved to: {lactate_summary_path}")
    print(lactate_summary)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 8: Stratified Tables by Onset Type
    """)
    return


@app.cell
def _(
    OUTPUT_DIR,
    SITE_NAME,
    analysis_df,
    pd,
    summarize_binary,
    summarize_continuous,
):
    # Create stratified tables - using a local function with unique internal variables

    def build_stratified_table(src_df, onset):
        """Create Table 1 for a specific onset type"""
        subset = src_df[src_df['type'] == onset].copy()
        if len(subset) == 0:
            return None

        grps = {
            'Total': subset,
            'Presumed Infection': subset[subset['group_presumed_infection']],
            'ASE with Lactate': subset[subset['group_ase_w_lactate']],
            'ASE without Lactate': subset[subset['group_ase_wo_lactate']]
        }

        rws = []
        rws.append({'Variable': 'N', **{n: str(len(g)) for n, g in grps.items()}})
        rws.append({'Variable': '--- Demographics ---', **{n: '' for n in grps}})
        rws.append({'Variable': 'Age, mean (SD)', **{n: summarize_continuous(g, 'age_at_admission') for n, g in grps.items()}})
        rws.append({'Variable': '--- Acuity ---', **{n: '' for n in grps}})
        rws.append({'Variable': 'CCI, mean (SD)', **{n: summarize_continuous(g, 'cci_score') for n, g in grps.items()}})
        rws.append({'Variable': 'Max SOFA, mean (SD)', **{n: summarize_continuous(g, 'max_sofa') for n, g in grps.items()}})
        rws.append({'Variable': '--- Life Support ---', **{n: '' for n in grps}})

        ls_vars = [('had_crrt', 'CRRT'), ('had_imv', 'IMV'), ('had_nippv', 'NIPPV'), ('had_hfno', 'HFNO'), ('had_vasopressor', 'Vasopressor')]
        rws.extend([{'Variable': f'{lb}, n (%)', **{n: summarize_binary(g, v) for n, g in grps.items()}} for v, lb in ls_vars])

        rws.append({'Variable': '--- Outcomes ---', **{n: '' for n in grps}})
        rws.append({'Variable': 'Hospital LOS, mean (SD)', **{n: summarize_continuous(g, 'hospital_los_days') for n, g in grps.items()}})
        rws.append({'Variable': 'In-hospital death, n (%)', **{n: summarize_binary(g, 'in_hospital_death') for n, g in grps.items()}})

        return pd.DataFrame(rws)

    # Create and save stratified tables
    strat_results = {}
    onset_types = ['community', 'hospital']
    strat_results = {ot: build_stratified_table(analysis_df, ot) for ot in onset_types}

    saved_strat_paths = []
    if strat_results.get('community') is not None:
        comm_path = OUTPUT_DIR / f"{SITE_NAME}_table1_community_onset.csv"
        strat_results['community'].to_csv(comm_path, index=False)
        saved_strat_paths.append(str(comm_path))
        print(f"Community-onset Table 1 saved: {comm_path}")
        print(f"  N = {len(analysis_df[analysis_df['type'] == 'community']):,}")
    else:
        print("No community-onset cases found")

    if strat_results.get('hospital') is not None:
        hosp_path = OUTPUT_DIR / f"{SITE_NAME}_table1_hospital_onset.csv"
        strat_results['hospital'].to_csv(hosp_path, index=False)
        saved_strat_paths.append(str(hosp_path))
        print(f"Hospital-onset Table 1 saved: {hosp_path}")
        print(f"  N = {len(analysis_df[analysis_df['type'] == 'hospital']):,}")
    else:
        print("No hospital-onset cases found")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Summary
    """)
    return


@app.cell
def _(analysis_df):
    print("=" * 60)
    print("Table 1 Generation Complete")
    print("=" * 60)
    print(f"Total hospitalizations: {len(analysis_df):,}")
    print(f"Presumed infection: {analysis_df['group_presumed_infection'].sum():,}")
    print(f"ASE with lactate: {analysis_df['group_ase_w_lactate'].sum():,}")
    print(f"ASE without lactate: {analysis_df['group_ase_wo_lactate'].sum():,}")
    if 'type' in analysis_df.columns:
        print(f"\nBy onset type:")
        print(f"  Community-onset: {(analysis_df['type'] == 'community').sum():,}")
        print(f"  Hospital-onset: {(analysis_df['type'] == 'hospital').sum():,}")
    return


if __name__ == "__main__":
    app.run()
