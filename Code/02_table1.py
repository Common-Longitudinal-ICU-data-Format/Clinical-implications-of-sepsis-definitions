import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # Table 1: Demographics and Outcomes by Sepsis Definition

    Compares demographics and outcomes across:
    1. No presumed infection (no blood culture + antibiotic criteria)
    2. Presumed infection
    3. ASE with lactate
    4. ASE without lactate
    5. Lactate-only ASE (qualified for ASE only because of lactate)
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
    PHI_DIR = Path(config["phi_directory"])
    SITE_NAME = config["site_name"]

    print(f"Site: {SITE_NAME}")
    print(f"Data directory: {DATA_DIR}")
    return DATA_DIR, FILETYPE, OUTPUT_DIR, PHI_DIR, SITE_NAME, TIMEZONE


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 1: Load Cohort and ASE Results
    """)
    return


@app.cell
def _(PHI_DIR, SITE_NAME, pd):
    # Load cohort and ASE results from notebook 1 (PHI data)
    cohort_df = pd.read_parquet(PHI_DIR / f"{SITE_NAME}_cohort_df.parquet")
    ase_df = pd.read_parquet(PHI_DIR / f"{SITE_NAME}_ase_results.parquet")

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

    # ICU types - dynamically detect all types present in the data
    # Deduplicate: each ICU type counted at most once per hospitalization
    icu_deduped = icu_stays[['hospitalization_id', 'location_type']].drop_duplicates()
    icu_types_in_data = sorted(icu_deduped['location_type'].dropna().unique().tolist())

    for icu_type in icu_types_in_data:
        hosp_with_type = set(icu_deduped[icu_deduped['location_type'] == icu_type]['hospitalization_id'])
        icu_df[icu_type] = icu_df['hospitalization_id'].isin(hosp_with_type)

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

    print(f"Microbiology computed: {(micro_df['positive_culture_count'] > 0).sum():,} patients with positive cultures")
    return (micro_df,)


@app.cell
def _(ase_df, hosp_ids, micro, pd):
    # Index BC analysis - match index blood culture to microbiology results
    micro_raw_idx = micro.df.copy()
    micro_raw_idx['collect_dttm'] = pd.to_datetime(micro_raw_idx['collect_dttm'], errors='coerce')

    # Get index BC times from ASE results
    index_bc_times = ase_df[['hospitalization_id', 'blood_culture_dttm']].copy()
    index_bc_times['blood_culture_dttm'] = pd.to_datetime(index_bc_times['blood_culture_dttm'], errors='coerce')

    # Merge micro data with index BC times
    micro_with_index = pd.merge(micro_raw_idx, index_bc_times, on='hospitalization_id', how='inner')

    # Calculate time difference from index BC (in days)
    micro_with_index['days_from_index'] = (
        (micro_with_index['collect_dttm'] - micro_with_index['blood_culture_dttm']).dt.total_seconds() / 86400
    )

    # Index BC: cultures collected on same day (within ~1 day tolerance for timing)
    index_bc_cultures = micro_with_index[abs(micro_with_index['days_from_index']) < 1].copy()

    # Window BC: cultures within ±2 days
    window_bc_cultures = micro_with_index[abs(micro_with_index['days_from_index']) <= 2].copy()

    # Determine if index BC was positive
    index_bc_positive = index_bc_cultures[
        (index_bc_cultures['organism_category'].notna()) &
        (index_bc_cultures['organism_category'] != 'no_growth')
    ]
    index_bc_positive_hosp = set(index_bc_positive['hospitalization_id'].unique())

    # Create index BC indicators dataframe
    index_bc_df = pd.DataFrame({'hospitalization_id': hosp_ids})
    index_bc_df['index_bc_positive'] = index_bc_df['hospitalization_id'].isin(index_bc_positive_hosp)

    # Count BCs in window per hospitalization
    window_bc_counts = window_bc_cultures.groupby('hospitalization_id').size().reset_index(name='bc_count_in_window')
    index_bc_df = pd.merge(index_bc_df, window_bc_counts, on='hospitalization_id', how='left')
    index_bc_df['bc_count_in_window'] = index_bc_df['bc_count_in_window'].fillna(0)

    # Count POSITIVE BCs in window per hospitalization
    window_positive_counts = window_bc_cultures[
        (window_bc_cultures['organism_category'].notna()) &
        (window_bc_cultures['organism_category'] != 'no_growth')
    ].groupby('hospitalization_id').size().reset_index(name='bc_positive_count_in_window')
    index_bc_df = pd.merge(index_bc_df, window_positive_counts, on='hospitalization_id', how='left')
    index_bc_df['bc_positive_count_in_window'] = index_bc_df['bc_positive_count_in_window'].fillna(0)

    # Top 20 organisms from positive INDEX blood cultures
    index_bc_organisms = index_bc_positive['organism_category'].value_counts().head(20)

    # Top 20 organisms from ALL positive cultures in window
    window_positive = window_bc_cultures[
        (window_bc_cultures['organism_category'].notna()) &
        (window_bc_cultures['organism_category'] != 'no_growth')
    ]
    window_bc_organisms = window_positive['organism_category'].value_counts().head(20)

    print(f"Index BC positive: {len(index_bc_positive_hosp):,} patients")
    print(f"Index BC organisms: {len(index_bc_organisms)} unique categories")
    print(f"Window BC organisms: {len(window_bc_organisms)} unique categories")
    return index_bc_df, index_bc_organisms, window_bc_organisms


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
def _(Path, cohort_df, json, pd):
    import polars as pl
    from clifpy import compute_sofa_polars

    # Load config
    sofa_cfg = json.loads(Path("clif_config.json").read_text())

    # Create cohort DataFrame for Polars
    sofa_cohort = pl.DataFrame({
        'hospitalization_id': cohort_df['hospitalization_id'].astype(str).tolist(),
        'start_dttm': pd.to_datetime(cohort_df['admission_dttm']).tolist(),
        'end_dttm': pd.to_datetime(cohort_df['discharge_dttm']).tolist()
    })

    print("Computing SOFA scores with Polars...")

    # Compute SOFA scores using high-performance Polars implementation
    sofa_scores_pl = compute_sofa_polars(
        data_directory=sofa_cfg["data_directory"],
        cohort_df=sofa_cohort,
        filetype=sofa_cfg["filetype"],
        timezone=sofa_cfg["timezone"],
        fill_na_scores_with_zero=True,
        remove_outliers=True
    )

    # Convert to pandas and get max SOFA per hospitalization
    sofa_scores = sofa_scores_pl.to_pandas()
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
    index_bc_df,
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
                'presumed_infection_onset_dttm',
                'ase_first_criteria_w_lactate', 'ase_first_criteria_wo_lactate',
                'vasopressor_name', 'total_qad']],
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

    # NEW: No presumed infection and lactate-only ASE groups
    analysis_df['group_no_presumed_infection'] = analysis_df['presumed_infection'] == 0
    analysis_df['group_lactate_only_ase'] = (
        (analysis_df['sepsis'] == 1) &
        (analysis_df['lactate_dttm'].notna()) &
        (analysis_df['vasopressor_dttm'].isna()) &
        (analysis_df['imv_dttm'].isna()) &
        (analysis_df['aki_dttm'].isna()) &
        (analysis_df['hyperbilirubinemia_dttm'].isna()) &
        (analysis_df['thrombocytopenia_dttm'].isna())
    )

    # Merge all derived DataFrames
    analysis_df = analysis_df.merge(icu_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(crrt_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(resp_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(vaso_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(micro_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(sofa_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(cci_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(first_vital_df, on='hospitalization_id', how='left')
    analysis_df = analysis_df.merge(index_bc_df, on='hospitalization_id', how='left')

    # Compute time from admission to first organ failure
    organ_failure_cols = ['aki_dttm', 'vasopressor_dttm', 'hyperbilirubinemia_dttm',
                          'thrombocytopenia_dttm', 'lactate_dttm', 'imv_dttm']
    # Convert to datetime if needed
    for col in organ_failure_cols:
        if col in analysis_df.columns:
            analysis_df[col] = pd.to_datetime(analysis_df[col], errors='coerce')
    analysis_df['first_vital_dttm'] = pd.to_datetime(analysis_df['first_vital_dttm'], errors='coerce')
    analysis_df['admission_dttm'] = pd.to_datetime(analysis_df['admission_dttm'], errors='coerce')

    # Get earliest organ failure datetime per patient
    analysis_df['first_organ_failure_dttm'] = analysis_df[organ_failure_cols].min(axis=1)

    # Calculate hours from admission to first organ failure
    analysis_df['time_to_organ_failure_hours'] = (
        (analysis_df['first_organ_failure_dttm'] - analysis_df['admission_dttm']).dt.total_seconds() / 3600
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
    analysis_df['had_vasopressor_ase'] = analysis_df['vasopressor_dttm'].notna()
    analysis_df['had_imv_ase'] = analysis_df['imv_dttm'].notna()

    # Count of organ dysfunctions per patient (6 CDC criteria)
    analysis_df['organ_dysfunction_count'] = (
        analysis_df['had_vasopressor_ase'].astype(int) +
        analysis_df['had_imv_ase'].astype(int) +
        analysis_df['had_aki'].astype(int) +
        analysis_df['had_hyperbili'].astype(int) +
        analysis_df['had_thrombocytopenia'].astype(int) +
        analysis_df['had_elevated_lactate'].astype(int)
    )

    # Rank organ failures by timestamp and compute time to 2nd organ failure
    def compute_organ_sequence(row, include_lactate=True):
        """Get ordered organ failure datetimes for a patient"""
        cols = ['vasopressor_dttm', 'imv_dttm', 'aki_dttm',
                'hyperbilirubinemia_dttm', 'thrombocytopenia_dttm']
        if include_lactate:
            cols.append('lactate_dttm')

        times = [(row[c], c) for c in cols if pd.notna(row.get(c))]
        times.sort(key=lambda x: x[0])
        return times

    def get_first_organ_dysfunction(row, include_lactate=True):
        """Return the name of the first organ dysfunction by datetime."""
        times = compute_organ_sequence(row, include_lactate=include_lactate)
        if times:
            return times[0][1].replace('_dttm', '')
        return None

    # Compute time from 1st to 2nd organ failure (with lactate)
    def time_to_second_organ(row):
        times = compute_organ_sequence(row, include_lactate=True)
        if len(times) >= 2:
            return (times[1][0] - times[0][0]).total_seconds() / 3600
        return None

    analysis_df['time_to_second_organ_hours'] = analysis_df.apply(time_to_second_organ, axis=1)

    analysis_df['first_organ_dysfunction_w_lactate'] = analysis_df.apply(
        lambda row: get_first_organ_dysfunction(row, include_lactate=True), axis=1
    )
    analysis_df['first_organ_dysfunction_wo_lactate'] = analysis_df.apply(
        lambda row: get_first_organ_dysfunction(row, include_lactate=False), axis=1
    )

    # Time from lactate (when it was first) to 2nd organ failure
    def lactate_to_second_organ(row):
        times = compute_organ_sequence(row, include_lactate=True)
        if len(times) >= 2 and times[0][1] == 'lactate_dttm':
            return (times[1][0] - times[0][0]).total_seconds() / 3600
        return None

    analysis_df['lactate_to_second_organ_hours'] = analysis_df.apply(lactate_to_second_organ, axis=1)

    # Time from non-lactate first organ failure to 2nd organ failure
    def non_lactate_to_second_organ(row):
        times = compute_organ_sequence(row, include_lactate=True)
        if len(times) >= 2 and times[0][1] != 'lactate_dttm':
            return (times[1][0] - times[0][0]).total_seconds() / 3600
        return None

    analysis_df['non_lactate_to_second_organ_hours'] = analysis_df.apply(non_lactate_to_second_organ, axis=1)

    # Compute sequential time differences for CSV
    sequence_data = []
    for _, seq_row in analysis_df[analysis_df['group_ase_w_lactate']].iterrows():
        # With lactate
        times_w = compute_organ_sequence(seq_row, include_lactate=True)
        for i in range(len(times_w) - 1):
            diff_hours = (times_w[i+1][0] - times_w[i][0]).total_seconds() / 3600
            sequence_data.append({
                'hospitalization_id': seq_row['hospitalization_id'],
                'transition': f'{i+1}_to_{i+2}',
                'lactate_included': True,
                'time_diff_hours': diff_hours,
                'from_organ': times_w[i][1].replace('_dttm', ''),
                'to_organ': times_w[i+1][1].replace('_dttm', '')
            })

        # Without lactate
        times_wo = compute_organ_sequence(seq_row, include_lactate=False)
        for i in range(len(times_wo) - 1):
            diff_hours = (times_wo[i+1][0] - times_wo[i][0]).total_seconds() / 3600
            sequence_data.append({
                'hospitalization_id': seq_row['hospitalization_id'],
                'transition': f'{i+1}_to_{i+2}',
                'lactate_included': False,
                'time_diff_hours': diff_hours,
                'from_organ': times_wo[i][1].replace('_dttm', ''),
                'to_organ': times_wo[i+1][1].replace('_dttm', '')
            })

    sequence_df = pd.DataFrame(sequence_data)

    # Aggregate for CSV output
    if len(sequence_df) > 0:
        sequence_summary = sequence_df.groupby(['transition', 'lactate_included']).agg(
            mean_hours=('time_diff_hours', 'mean'),
            sd_hours=('time_diff_hours', 'std'),
            median_hours=('time_diff_hours', 'median'),
            q25_hours=('time_diff_hours', lambda x: x.quantile(0.25)),
            q75_hours=('time_diff_hours', lambda x: x.quantile(0.75)),
            n=('time_diff_hours', 'count')
        ).reset_index()
    else:
        sequence_summary = pd.DataFrame(columns=['transition', 'lactate_included', 'mean_hours', 'sd_hours', 'median_hours', 'q25_hours', 'q75_hours', 'n'])

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
    print(f"No presumed infection: {analysis_df['group_no_presumed_infection'].sum():,}")
    print(f"Presumed infection: {analysis_df['group_presumed_infection'].sum():,}")
    print(f"ASE with lactate: {analysis_df['group_ase_w_lactate'].sum():,}")
    print(f"ASE without lactate: {analysis_df['group_ase_wo_lactate'].sum():,}")
    print(f"Lactate-only ASE: {analysis_df['group_lactate_only_ase'].sum():,}")
    return analysis_df, sequence_summary


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
def _():
    import math

    def compute_table1_json(groups, site_name, table_type):
        """Compute raw numeric Table 1 values for multi-site aggregation.

        Parameters
        ----------
        groups : dict[str, DataFrame]
            Mapping of group name to DataFrame.
        site_name : str
            Site identifier (e.g. 'mimic').
        table_type : str
            Table identifier (e.g. 'overall', 'community_onset').

        Returns
        -------
        dict
            Structured JSON-serialisable dict with raw numeric values.
        """

        def _safe(v):
            """Convert numpy/pandas types to JSON-safe Python types."""
            if v is None:
                return None
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            if hasattr(v, 'item'):
                return v.item()
            return v

        def cont(df, col):
            """Summarise a continuous variable."""
            vals = df[col].dropna()
            if len(vals) == 0:
                return None
            return {
                "n": int(len(vals)),
                "mean": _safe(vals.mean()),
                "sd": _safe(vals.std()),
                "median": _safe(vals.median()),
                "q25": _safe(vals.quantile(0.25)),
                "q75": _safe(vals.quantile(0.75)),
            }

        def binary(df, col_or_mask):
            """Summarise a binary variable."""
            if isinstance(col_or_mask, str):
                count = int(df[col_or_mask].sum())
            else:
                count = int(col_or_mask.sum())
            return {"count": count, "total": len(df)}

        def categ(df, col, categories):
            """Summarise a categorical variable."""
            total = len(df)
            return {
                cat: {"count": int((df[col] == cat).sum()), "total": total}
                for cat in categories
            }

        # Group-applicability sets
        pi_group_names = {'Presumed Infection', 'ASE with Lactate',
                          'ASE without Lactate', 'Lactate-only ASE'}
        ase_group_names = {'ASE with Lactate', 'ASE without Lactate',
                           'Lactate-only ASE'}
        lactate_group_names = {'ASE with Lactate', 'Lactate-only ASE'}
        qad_excluded = {'Total', 'No Presumed Infection'}

        # Derive category lists from the Total group
        total_df = groups.get('Total', next(iter(groups.values())))
        race_eth_cats = ['Non-Hispanic White', 'Non-Hispanic Black',
                         'Hispanic White', 'Hispanic Black', 'Asian', 'Other']
        race_cats = sorted(total_df['race_category'].dropna().unique().tolist())
        eth_cats = sorted(total_df['ethnicity_category'].dropna().unique().tolist())
        first_w = ['vasopressor', 'imv', 'aki', 'hyperbilirubinemia',
                    'thrombocytopenia', 'lactate']
        first_wo = ['vasopressor', 'imv', 'aki', 'hyperbilirubinemia',
                     'thrombocytopenia']
        ase_onset_w = ['blood_culture', 'first_qad', 'vasopressor', 'imv', 'aki',
                       'hyperbilirubinemia', 'thrombocytopenia', 'lactate']
        ase_onset_wo = ['blood_culture', 'first_qad', 'vasopressor', 'imv', 'aki',
                        'hyperbilirubinemia', 'thrombocytopenia']

        result = {"site_name": site_name, "table_type": table_type, "groups": {}}

        for name, df in groups.items():
            is_pi = name in pi_group_names
            is_ase = name in ase_group_names
            is_lac = name in lactate_group_names
            has_qad = name not in qad_excluded

            g = {"n": len(df), "continuous": {}, "binary": {}, "categorical": {}}

            # Demographics
            g["continuous"]["age_at_admission"] = cont(df, "age_at_admission")
            g["binary"]["sex_male"] = binary(df, df['sex_category'].str.lower() == 'male')
            g["binary"]["sex_female"] = binary(df, df['sex_category'].str.lower() == 'female')
            g["categorical"]["race_ethnicity"] = categ(df, "race_ethnicity", race_eth_cats)
            g["categorical"]["race_category"] = categ(df, "race_category", race_cats)
            g["categorical"]["ethnicity_category"] = categ(df, "ethnicity_category", eth_cats)

            # Comorbidities
            g["continuous"]["cci_score"] = cont(df, "cci_score")
            g["continuous"]["total_qad"] = cont(df, "total_qad") if has_qad else None

            # Acuity
            g["continuous"]["max_sofa"] = cont(df, "max_sofa")

            # ICU
            g["binary"]["had_icu"] = binary(df, "had_icu")
            icu_type_cols = sorted([c for c in df.columns if c.endswith('_icu') and c not in ('had_icu',)])
            for icu_col in icu_type_cols:
                g["binary"][icu_col] = binary(df, icu_col)
            icu_sub = df[df['had_icu']]
            g["continuous"]["icu_los_days"] = cont(icu_sub, "icu_los_days") if len(icu_sub) > 0 else None

            # Life Support
            g["binary"]["had_crrt"] = binary(df, "had_crrt")
            g["binary"]["had_imv"] = binary(df, "had_imv")
            g["binary"]["had_nippv"] = binary(df, "had_nippv")
            g["binary"]["had_hfno"] = binary(df, "had_hfno")
            g["binary"]["had_vasopressor"] = binary(df, "had_vasopressor")
            g["binary"]["any_life_support"] = binary(
                df,
                df['had_crrt'] | df['had_imv'] | df['had_nippv'] |
                df['had_hfno'] | df['had_vasopressor']
            )

            # Organ Failure (CDC) – ASE groups only
            for of_col in ['had_vasopressor_ase', 'had_imv_ase', 'had_aki',
                           'had_hyperbili', 'had_thrombocytopenia', 'had_elevated_lactate']:
                g["binary"][of_col] = binary(df, of_col) if is_ase else None
            g["continuous"]["organ_dysfunction_count"] = (
                cont(df, "organ_dysfunction_count") if is_ase else None
            )

            # Timing – ASE / lactate groups
            g["continuous"]["time_to_organ_failure_hours"] = (
                cont(df, "time_to_organ_failure_hours") if is_ase else None
            )
            g["continuous"]["time_to_second_organ_hours"] = (
                cont(df, "time_to_second_organ_hours") if is_ase else None
            )
            g["continuous"]["lactate_to_second_organ_hours"] = (
                cont(df, "lactate_to_second_organ_hours") if is_lac else None
            )
            g["continuous"]["non_lactate_to_second_organ_hours"] = (
                cont(df, "non_lactate_to_second_organ_hours") if is_ase else None
            )

            # First Organ – ASE groups only
            g["categorical"]["first_organ_dysfunction_w_lactate"] = (
                categ(df, "first_organ_dysfunction_w_lactate", first_w) if is_ase else None
            )
            g["categorical"]["first_organ_dysfunction_wo_lactate"] = (
                categ(df, "first_organ_dysfunction_wo_lactate", first_wo) if is_ase else None
            )

            # ASE Onset Criteria – ASE groups only
            g["categorical"]["ase_first_criteria_w_lactate"] = (
                categ(df, "ase_first_criteria_w_lactate", ase_onset_w) if is_ase else None
            )
            g["categorical"]["ase_first_criteria_wo_lactate"] = (
                categ(df, "ase_first_criteria_wo_lactate", ase_onset_wo) if is_ase else None
            )

            # Outcomes
            g["continuous"]["hospital_los_days"] = cont(df, "hospital_los_days")
            g["binary"]["in_hospital_death"] = binary(df, "in_hospital_death")

            # Microbiology
            g["binary"]["index_bc_positive"] = binary(df, "index_bc_positive") if is_pi else None
            g["continuous"]["bc_count_in_window"] = (
                cont(df, "bc_count_in_window") if is_pi else None
            )
            g["continuous"]["bc_positive_count_in_window"] = (
                cont(df, "bc_positive_count_in_window") if is_pi else None
            )
            g["continuous"]["time_to_bc_hours"] = cont(df, "time_to_bc_hours")

            result["groups"][name] = g

        return result

    return (compute_table1_json,)


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
        'No Presumed Infection': analysis_df[analysis_df['group_no_presumed_infection']],
        'Presumed Infection': analysis_df[analysis_df['group_presumed_infection']],
        'ASE with Lactate': analysis_df[analysis_df['group_ase_w_lactate']],
        'ASE without Lactate': analysis_df[analysis_df['group_ase_wo_lactate']],
        'Lactate-only ASE': analysis_df[analysis_df['group_lactate_only_ase']]
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

    # Race/Ethnicity categories
    table1_rows.append({'Variable': '--- Race/Ethnicity ---', **{nm: '' for nm in groups}})
    race_eth_cats = ['Non-Hispanic White', 'Non-Hispanic Black', 'Hispanic White', 'Hispanic Black', 'Asian', 'Other']
    table1_rows.extend([
        {'Variable': f'{rc}, n (%)', **{nm: summarize_binary(df, df['race_ethnicity'] == rc) for nm, df in groups.items()}}
        for rc in race_eth_cats
    ])

    # Raw Race
    table1_rows.append({'Variable': '--- Race ---', **{nm: '' for nm in groups}})
    race_cats = analysis_df['race_category'].dropna().unique().tolist()
    table1_rows.extend([
        {'Variable': f'{rc}, n (%)', **{nm: summarize_binary(df, df['race_category'] == rc) for nm, df in groups.items()}}
        for rc in race_cats
    ])

    # Raw Ethnicity
    table1_rows.append({'Variable': '--- Ethnicity ---', **{nm: '' for nm in groups}})
    eth_cats = analysis_df['ethnicity_category'].dropna().unique().tolist()
    table1_rows.extend([
        {'Variable': f'{ec}, n (%)', **{nm: summarize_binary(df, df['ethnicity_category'] == ec) for nm, df in groups.items()}}
        for ec in eth_cats
    ])

    # Comorbidities
    table1_rows.append({'Variable': '--- Comorbidities ---', **{nm: '' for nm in groups}})
    table1_rows.append({'Variable': 'CCI, mean (SD)', **{nm: summarize_continuous(df, 'cci_score') for nm, df in groups.items()}})

    # QADs - only for groups with presumed infection (not Total or No Presumed Infection)
    qad_excluded_groups = {'Total', 'No Presumed Infection'}
    row = {'Variable': 'QADs, mean (SD)'}
    for nm, df in groups.items():
        if nm in qad_excluded_groups:
            row[nm] = 'N/A'
        else:
            row[nm] = summarize_continuous(df, 'total_qad')
    table1_rows.append(row)

    # Acuity
    table1_rows.append({'Variable': '--- Acuity ---', **{nm: '' for nm in groups}})
    table1_rows.append({'Variable': 'Max SOFA, mean (SD)', **{nm: summarize_continuous(df, 'max_sofa') for nm, df in groups.items()}})

    # ICU
    table1_rows.append({'Variable': '--- ICU ---', **{nm: '' for nm in groups}})
    table1_rows.append({'Variable': 'Any ICU, n (%)', **{nm: summarize_binary(df, 'had_icu') for nm, df in groups.items()}})

    icu_type_cols = sorted([c for c in analysis_df.columns if c.endswith('_icu') and c not in ('had_icu',)])
    table1_rows.extend([
        {'Variable': f'{col}, n (%)', **{nm: summarize_binary(df, col) for nm, df in groups.items()}}
        for col in icu_type_cols
    ])

    # Life Support
    table1_rows.append({'Variable': '--- Life Support ---', **{nm: '' for nm in groups}})
    # Any Life Support (any of CRRT, IMV, NIPPV, HFNO, Vasopressor)
    table1_rows.append({
        'Variable': 'Any Life Support, n (%)',
        **{nm: summarize_binary(df, df['had_crrt'] | df['had_imv'] | df['had_nippv'] | df['had_hfno'] | df['had_vasopressor']) for nm, df in groups.items()}
    })
    life_support = [('had_crrt', 'CRRT'), ('had_imv', 'IMV'), ('had_nippv', 'NIPPV'), ('had_hfno', 'HFNO'), ('had_vasopressor', 'Vasopressor')]
    table1_rows.extend([
        {'Variable': f'{lbl}, n (%)', **{nm: summarize_binary(df, vr) for nm, df in groups.items()}}
        for vr, lbl in life_support
    ])

    # Organ Failure (CDC) - Only applicable to ASE groups
    table1_rows.append({'Variable': '--- Organ Failure (CDC) ---', **{nm: '' for nm in groups}})

    # Define ASE-only groups (organ failure only relevant for these)
    ase_group_names = {'ASE with Lactate', 'ASE without Lactate', 'Lactate-only ASE'}

    # All 6 CDC organ dysfunction criteria (using ASE-criteria-specific indicators)
    organ_failure = [
        ('had_vasopressor_ase', 'Vasopressor'),
        ('had_imv_ase', 'IMV'),
        ('had_aki', 'AKI'),
        ('had_hyperbili', 'Hyperbilirubinemia'),
        ('had_thrombocytopenia', 'Thrombocytopenia'),
        ('had_elevated_lactate', 'Elevated Lactate')
    ]

    for vr, lbl in organ_failure:
        row = {'Variable': f'{lbl}, n (%)'}
        for nm, df in groups.items():
            if nm in ase_group_names:
                row[nm] = summarize_binary(df, vr)
            else:
                row[nm] = 'N/A'
        table1_rows.append(row)

    # Mean organ dysfunction count - only for ASE groups
    row = {'Variable': 'Organ dysfunctions, mean (SD)'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            row[nm] = summarize_continuous(df, 'organ_dysfunction_count')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # Median organ dysfunction count - only for ASE groups
    row = {'Variable': 'Organ dysfunctions, median (IQR)'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            row[nm] = summarize_median_iqr(df, 'organ_dysfunction_count')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # Adm to 1st organ failure (also only for ASE groups)
    row = {'Variable': 'Adm to 1st organ failure (hours), median (IQR)'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            row[nm] = summarize_median_iqr(df, 'time_to_organ_failure_hours')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # Adm to 1st organ failure mean
    row = {'Variable': 'Adm to 1st organ failure (hours), mean (SD)'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            vals = df['time_to_organ_failure_hours'].dropna()
            row[nm] = f"{vals.mean():.1f} ({vals.std():.1f})" if len(vals) > 0 else 'N/A'
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # 1st to 2nd organ failure (also only for ASE groups)
    row = {'Variable': '1st to 2nd organ failure (hours), median (IQR)'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            row[nm] = summarize_median_iqr(df, 'time_to_second_organ_hours')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # 1st to 2nd organ failure mean
    row = {'Variable': '1st to 2nd organ failure (hours), mean (SD)'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            vals = df['time_to_second_organ_hours'].dropna()
            row[nm] = f"{vals.mean():.1f} ({vals.std():.1f})" if len(vals) > 0 else 'N/A'
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # Lactate subgroup analysis - only for groups that include lactate
    lactate_group_names = {'ASE with Lactate', 'Lactate-only ASE'}

    # N where lactate was first ASE criterion
    row = {'Variable': 'N 1st lactate ASE onsets, n (%)'}
    for nm, df in groups.items():
        if nm in lactate_group_names:
            row[nm] = summarize_binary(df, df['ase_first_criteria_w_lactate'] == 'lactate')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # Lactate to 2nd organ failure median
    row = {'Variable': 'Lactate to 2nd organ failure (hours), median (IQR)'}
    for nm, df in groups.items():
        if nm in lactate_group_names:
            row[nm] = summarize_median_iqr(df, 'lactate_to_second_organ_hours')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # Lactate to 2nd organ failure mean
    row = {'Variable': 'Lactate to 2nd organ failure (hours), mean (SD)'}
    for nm, df in groups.items():
        if nm in lactate_group_names:
            vals = df['lactate_to_second_organ_hours'].dropna()
            row[nm] = f"{vals.mean():.1f} ({vals.std():.1f})" if len(vals) > 0 else 'N/A'
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # Non-lactate 1st to 2nd organ failure median
    row = {'Variable': 'Non-lactate 1st to 2nd organ failure (hours), median (IQR)'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            row[nm] = summarize_median_iqr(df, 'non_lactate_to_second_organ_hours')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # Non-lactate 1st to 2nd organ failure mean
    row = {'Variable': 'Non-lactate 1st to 2nd organ failure (hours), mean (SD)'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            vals = df['non_lactate_to_second_organ_hours'].dropna()
            row[nm] = f"{vals.mean():.1f} ({vals.std():.1f})" if len(vals) > 0 else 'N/A'
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # First Organ Dysfunction Distribution - Only applicable to ASE groups
    table1_rows.append({'Variable': '--- First Organ Dysfunction ---', **{nm: '' for nm in groups}})

    # Helper function to summarize first organ dysfunction
    def summarize_first_organ_dysfunction(df, col):
        """Summarize first organ dysfunction as most common type"""
        if col not in df.columns:
            return "N/A"
        vals = df[col].dropna()
        if len(vals) == 0:
            return "N/A"
        counts = vals.value_counts()
        if len(counts) == 0:
            return "N/A"
        top = counts.index[0]
        n = counts.iloc[0]
        total = len(vals)
        return f"{top}: {n} ({100*n/total:.1f}%)"

    # First criteria for ASE with lactate (only for ASE groups)
    row = {'Variable': 'First organ dysfunction (w/ lactate), top'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            row[nm] = summarize_first_organ_dysfunction(df, 'first_organ_dysfunction_w_lactate')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # Breakdown: first criterion (w/ lactate) by organ failure
    first_criteria_w_lactate = [
        ('vasopressor', 'Vasopressor'),
        ('imv', 'IMV'),
        ('aki', 'AKI'),
        ('hyperbilirubinemia', 'Hyperbilirubinemia'),
        ('thrombocytopenia', 'Thrombocytopenia'),
        ('lactate', 'Lactate'),
    ]
    for val, lbl in first_criteria_w_lactate:
        row = {'Variable': f'  {lbl}, n (%)'}
        for nm, df in groups.items():
            if nm in ase_group_names:
                row[nm] = summarize_binary(df, df['first_organ_dysfunction_w_lactate'] == val)
            else:
                row[nm] = 'N/A'
        table1_rows.append(row)

    # First criteria for ASE without lactate (only for ASE groups)
    row = {'Variable': 'First organ dysfunction (w/o lactate), top'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            row[nm] = summarize_first_organ_dysfunction(df, 'first_organ_dysfunction_wo_lactate')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # Breakdown: first criterion (w/o lactate) by organ failure
    first_criteria_wo_lactate = [
        ('vasopressor', 'Vasopressor'),
        ('imv', 'IMV'),
        ('aki', 'AKI'),
        ('hyperbilirubinemia', 'Hyperbilirubinemia'),
        ('thrombocytopenia', 'Thrombocytopenia'),
    ]
    for val, lbl in first_criteria_wo_lactate:
        row = {'Variable': f'  {lbl}, n (%)'}
        for nm, df in groups.items():
            if nm in ase_group_names:
                row[nm] = summarize_binary(df, df['first_organ_dysfunction_wo_lactate'] == val)
            else:
                row[nm] = 'N/A'
        table1_rows.append(row)

    # ASE onset criteria with lactate (only for ASE groups)
    row = {'Variable': 'ASE onset criteria (w/ lactate), top'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            row[nm] = summarize_first_organ_dysfunction(df, 'ase_first_criteria_w_lactate')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    ase_criteria_w_lactate = [
        ('blood_culture', 'Blood Culture'),
        ('first_qad', 'First QAD'),
        ('vasopressor', 'Vasopressor'),
        ('imv', 'IMV'),
        ('aki', 'AKI'),
        ('hyperbilirubinemia', 'Hyperbilirubinemia'),
        ('thrombocytopenia', 'Thrombocytopenia'),
        ('lactate', 'Lactate'),
    ]
    for val, lbl in ase_criteria_w_lactate:
        row = {'Variable': f'  {lbl}, n (%)'}
        for nm, df in groups.items():
            if nm in ase_group_names:
                row[nm] = summarize_binary(df, df['ase_first_criteria_w_lactate'] == val)
            else:
                row[nm] = 'N/A'
        table1_rows.append(row)

    # ASE onset criteria without lactate (only for ASE groups)
    row = {'Variable': 'ASE onset criteria (w/o lactate), top'}
    for nm, df in groups.items():
        if nm in ase_group_names:
            row[nm] = summarize_first_organ_dysfunction(df, 'ase_first_criteria_wo_lactate')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    ase_criteria_wo_lactate = [
        ('blood_culture', 'Blood Culture'),
        ('first_qad', 'First QAD'),
        ('vasopressor', 'Vasopressor'),
        ('imv', 'IMV'),
        ('aki', 'AKI'),
        ('hyperbilirubinemia', 'Hyperbilirubinemia'),
        ('thrombocytopenia', 'Thrombocytopenia'),
    ]
    for val, lbl in ase_criteria_wo_lactate:
        row = {'Variable': f'  {lbl}, n (%)'}
        for nm, df in groups.items():
            if nm in ase_group_names:
                row[nm] = summarize_binary(df, df['ase_first_criteria_wo_lactate'] == val)
            else:
                row[nm] = 'N/A'
        table1_rows.append(row)

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

    # Index BC positivity - only for groups with presumed infection
    pi_groups = {'Presumed Infection', 'ASE with Lactate', 'ASE without Lactate', 'Lactate-only ASE'}
    row = {'Variable': 'Positive index BC, n (%)'}
    for nm, df in groups.items():
        if nm in pi_groups:
            row[nm] = summarize_binary(df, 'index_bc_positive')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # BCs in index window - only for groups with presumed infection
    row = {'Variable': 'BCs in index window, mean (SD)'}
    for nm, df in groups.items():
        if nm in pi_groups:
            row[nm] = summarize_continuous(df, 'bc_count_in_window')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    # Positive BCs in window - only for groups with presumed infection
    row = {'Variable': 'Positive BCs in window, mean (SD)'}
    for nm, df in groups.items():
        if nm in pi_groups:
            row[nm] = summarize_continuous(df, 'bc_positive_count_in_window')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

    row = {'Variable': 'Positive BCs in window, median (IQR)'}
    for nm, df in groups.items():
        if nm in pi_groups:
            row[nm] = summarize_median_iqr(df, 'bc_positive_count_in_window')
        else:
            row[nm] = 'N/A'
    table1_rows.append(row)

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
def _(
    OUTPUT_DIR,
    SITE_NAME,
    index_bc_organisms,
    sequence_summary,
    table1,
    window_bc_organisms,
):
    # Save main Table 1
    table1_path = OUTPUT_DIR / f"{SITE_NAME}_table1.csv"
    table1.to_csv(table1_path, index=False)
    print(f"Table 1 saved to: {table1_path}")

    # Save index BC organisms (positive index blood cultures only)
    index_bc_org_path = OUTPUT_DIR / f"{SITE_NAME}_index_bc_top20_organisms.csv"
    index_bc_organisms.to_csv(index_bc_org_path)
    print(f"Index BC organisms saved to: {index_bc_org_path}")

    # Save window BC organisms (all positive cultures in ±2 day window)
    window_bc_org_path = OUTPUT_DIR / f"{SITE_NAME}_window_bc_top20_organisms.csv"
    window_bc_organisms.to_csv(window_bc_org_path)
    print(f"Window BC organisms saved to: {window_bc_org_path}")

    # Save organ failure sequence times
    sequence_path = OUTPUT_DIR / f"{SITE_NAME}_organ_failure_sequence_times.csv"
    sequence_summary.to_csv(sequence_path, index=False)
    print(f"Organ failure sequence times saved to: {sequence_path}")
    return


@app.cell
def _(OUTPUT_DIR, SITE_NAME, analysis_df, compute_table1_json, json):
    # Save Table 1 as structured JSON for multi-site aggregation
    _groups = {
        'Total': analysis_df,
        'No Presumed Infection': analysis_df[analysis_df['group_no_presumed_infection']],
        'Presumed Infection': analysis_df[analysis_df['group_presumed_infection']],
        'ASE with Lactate': analysis_df[analysis_df['group_ase_w_lactate']],
        'ASE without Lactate': analysis_df[analysis_df['group_ase_wo_lactate']],
        'Lactate-only ASE': analysis_df[analysis_df['group_lactate_only_ase']]
    }
    table1_json = compute_table1_json(_groups, SITE_NAME, "overall")
    table1_json_path = OUTPUT_DIR / f"{SITE_NAME}_table1.json"
    table1_json_path.write_text(json.dumps(table1_json, indent=2))
    print(f"Table 1 JSON saved to: {table1_json_path}")
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
        analysis_df[['hospitalization_id', 'admission_dttm', 'group_no_presumed_infection', 'group_presumed_infection', 'group_ase_w_lactate', 'group_ase_wo_lactate', 'group_lactate_only_ase']],
        on='hospitalization_id',
        how='left'
    )

    # Derive year_month from admission_dttm
    lactate_counts_with_groups['year_month'] = pd.to_datetime(
        lactate_counts_with_groups['admission_dttm']
    ).dt.to_period('M').astype(str)

    # Group definitions: group_name -> (flag_column, lactate_count_column or None)
    group_defs = {
        'No Presumed Infection': ('group_no_presumed_infection', None),
        'Presumed Infection': ('group_presumed_infection', 'lactates_before_presumed_infection'),
        'ASE with Lactate': ('group_ase_w_lactate', 'lactates_before_ase_w_lactate'),
        'ASE without Lactate': ('group_ase_wo_lactate', 'lactates_before_ase_wo_lactate'),
        'Lactate-only ASE': ('group_lactate_only_ase', 'lactates_before_ase_w_lactate'),
    }

    # Create summary pivoted by year_month (rows) x groups (columns)
    lactate_summary_rows = []
    for ym in sorted(lactate_counts_with_groups['year_month'].unique()):
        ym_data = lactate_counts_with_groups[lactate_counts_with_groups['year_month'] == ym]
        _row = {'year_month': ym}
        for group_name, (flag_col, lactate_col) in group_defs.items():
            subset = ym_data[ym_data[flag_col]]
            _row[f'{group_name}_N'] = len(subset)
            if lactate_col:
                _row[f'{group_name}_lactates_median_iqr'] = summarize_median_iqr(subset, lactate_col)
            else:
                _row[f'{group_name}_lactates_median_iqr'] = 'N/A'
        lactate_summary_rows.append(_row)

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
    compute_table1_json,
    json,
    pd,
    summarize_binary,
    summarize_continuous,
    summarize_median_iqr,
):
    # Create stratified tables - using a local function with unique internal variables

    def build_stratified_table(src_df, onset):
        """Create Table 1 for a specific onset type"""
        subset = src_df[src_df['type'] == onset].copy()
        if len(subset) == 0:
            return None

        grps = {
            'Total': subset,
            'No Presumed Infection': subset[subset['group_no_presumed_infection']],
            'Presumed Infection': subset[subset['group_presumed_infection']],
            'ASE with Lactate': subset[subset['group_ase_w_lactate']],
            'ASE without Lactate': subset[subset['group_ase_wo_lactate']],
            'Lactate-only ASE': subset[subset['group_lactate_only_ase']]
        }

        rws = []

        # N
        rws.append({'Variable': 'N', **{n: str(len(g)) for n, g in grps.items()}})

        # Demographics
        rws.append({'Variable': '--- Demographics ---', **{n: '' for n in grps}})
        rws.append({'Variable': 'Age, mean (SD)', **{n: summarize_continuous(g, 'age_at_admission') for n, g in grps.items()}})
        rws.append({'Variable': 'Sex - Male, n (%)', **{n: summarize_binary(g, g['sex_category'].str.lower() == 'male') for n, g in grps.items()}})
        rws.append({'Variable': 'Sex - Female, n (%)', **{n: summarize_binary(g, g['sex_category'].str.lower() == 'female') for n, g in grps.items()}})

        # Race/Ethnicity
        rws.append({'Variable': '--- Race/Ethnicity ---', **{n: '' for n in grps}})
        race_eth_cats = ['Non-Hispanic White', 'Non-Hispanic Black', 'Hispanic White', 'Hispanic Black', 'Asian', 'Other']
        rws.extend([
            {'Variable': f'{rc}, n (%)', **{n: summarize_binary(g, g['race_ethnicity'] == rc) for n, g in grps.items()}}
            for rc in race_eth_cats
        ])

        # Raw Race
        rws.append({'Variable': '--- Race ---', **{n: '' for n in grps}})
        race_cats = subset['race_category'].dropna().unique().tolist()
        rws.extend([
            {'Variable': f'{rc}, n (%)', **{n: summarize_binary(g, g['race_category'] == rc) for n, g in grps.items()}}
            for rc in race_cats
        ])

        # Raw Ethnicity
        rws.append({'Variable': '--- Ethnicity ---', **{n: '' for n in grps}})
        eth_cats = subset['ethnicity_category'].dropna().unique().tolist()
        rws.extend([
            {'Variable': f'{ec}, n (%)', **{n: summarize_binary(g, g['ethnicity_category'] == ec) for n, g in grps.items()}}
            for ec in eth_cats
        ])

        # Comorbidities
        rws.append({'Variable': '--- Comorbidities ---', **{n: '' for n in grps}})
        rws.append({'Variable': 'CCI, mean (SD)', **{n: summarize_continuous(g, 'cci_score') for n, g in grps.items()}})

        # QADs - only for groups with presumed infection (not Total or No Presumed Infection)
        qad_excluded_groups = {'Total', 'No Presumed Infection'}
        row = {'Variable': 'QADs, mean (SD)'}
        for n, g in grps.items():
            if n in qad_excluded_groups:
                row[n] = 'N/A'
            else:
                row[n] = summarize_continuous(g, 'total_qad')
        rws.append(row)

        # Acuity
        rws.append({'Variable': '--- Acuity ---', **{n: '' for n in grps}})
        rws.append({'Variable': 'Max SOFA, mean (SD)', **{n: summarize_continuous(g, 'max_sofa') for n, g in grps.items()}})

        # ICU
        rws.append({'Variable': '--- ICU ---', **{n: '' for n in grps}})
        rws.append({'Variable': 'Any ICU, n (%)', **{n: summarize_binary(g, 'had_icu') for n, g in grps.items()}})
        icu_type_cols = sorted([c for c in subset.columns if c.endswith('_icu') and c not in ('had_icu',)])
        rws.extend([
            {'Variable': f'{col}, n (%)', **{n: summarize_binary(g, col) for n, g in grps.items()}}
            for col in icu_type_cols
        ])

        # Life Support
        rws.append({'Variable': '--- Life Support ---', **{n: '' for n in grps}})
        # Any Life Support (any of CRRT, IMV, NIPPV, HFNO, Vasopressor)
        rws.append({
            'Variable': 'Any Life Support, n (%)',
            **{n: summarize_binary(g, g['had_crrt'] | g['had_imv'] | g['had_nippv'] | g['had_hfno'] | g['had_vasopressor']) for n, g in grps.items()}
        })
        ls_vars = [('had_crrt', 'CRRT'), ('had_imv', 'IMV'), ('had_nippv', 'NIPPV'), ('had_hfno', 'HFNO'), ('had_vasopressor', 'Vasopressor')]
        rws.extend([{'Variable': f'{lb}, n (%)', **{n: summarize_binary(g, v) for n, g in grps.items()}} for v, lb in ls_vars])

        # Organ Failure (CDC) - Only applicable to ASE groups
        rws.append({'Variable': '--- Organ Failure (CDC) ---', **{n: '' for n in grps}})
        ase_group_names = {'ASE with Lactate', 'ASE without Lactate', 'Lactate-only ASE'}
        organ_failure = [
            ('had_vasopressor_ase', 'Vasopressor'),
            ('had_imv_ase', 'IMV'),
            ('had_aki', 'AKI'),
            ('had_hyperbili', 'Hyperbilirubinemia'),
            ('had_thrombocytopenia', 'Thrombocytopenia'),
            ('had_elevated_lactate', 'Elevated Lactate')
        ]
        for vr, lbl in organ_failure:
            row = {'Variable': f'{lbl}, n (%)'}
            for n, g in grps.items():
                row[n] = summarize_binary(g, vr) if n in ase_group_names else 'N/A'
            rws.append(row)

        # Mean organ dysfunction count - only for ASE groups
        row = {'Variable': 'Organ dysfunctions, mean (SD)'}
        for n, g in grps.items():
            if n in ase_group_names:
                row[n] = summarize_continuous(g, 'organ_dysfunction_count')
            else:
                row[n] = 'N/A'
        rws.append(row)

        # Median organ dysfunction count - only for ASE groups
        row = {'Variable': 'Organ dysfunctions, median (IQR)'}
        for n, g in grps.items():
            row[n] = summarize_median_iqr(g, 'organ_dysfunction_count') if n in ase_group_names else 'N/A'
        rws.append(row)

        # Adm to 1st organ failure (ASE groups only)
        row = {'Variable': 'Adm to 1st organ failure (hours), median (IQR)'}
        for n, g in grps.items():
            row[n] = summarize_median_iqr(g, 'time_to_organ_failure_hours') if n in ase_group_names else 'N/A'
        rws.append(row)

        # Adm to 1st organ failure mean
        row = {'Variable': 'Adm to 1st organ failure (hours), mean (SD)'}
        for n, g in grps.items():
            if n in ase_group_names:
                vals = g['time_to_organ_failure_hours'].dropna()
                row[n] = f"{vals.mean():.1f} ({vals.std():.1f})" if len(vals) > 0 else 'N/A'
            else:
                row[n] = 'N/A'
        rws.append(row)

        # 1st to 2nd organ failure (ASE groups only)
        row = {'Variable': '1st to 2nd organ failure (hours), median (IQR)'}
        for n, g in grps.items():
            row[n] = summarize_median_iqr(g, 'time_to_second_organ_hours') if n in ase_group_names else 'N/A'
        rws.append(row)

        # 1st to 2nd organ failure mean
        row = {'Variable': '1st to 2nd organ failure (hours), mean (SD)'}
        for n, g in grps.items():
            if n in ase_group_names:
                vals = g['time_to_second_organ_hours'].dropna()
                row[n] = f"{vals.mean():.1f} ({vals.std():.1f})" if len(vals) > 0 else 'N/A'
            else:
                row[n] = 'N/A'
        rws.append(row)

        # Lactate subgroup analysis - only for groups that include lactate
        lactate_group_names = {'ASE with Lactate', 'Lactate-only ASE'}

        # N where lactate was first ASE criterion
        row = {'Variable': 'N 1st lactate ASE onsets, n (%)'}
        for n, g in grps.items():
            if n in lactate_group_names:
                row[n] = summarize_binary(g, g['ase_first_criteria_w_lactate'] == 'lactate')
            else:
                row[n] = 'N/A'
        rws.append(row)

        # Lactate to 2nd organ failure median
        row = {'Variable': 'Lactate to 2nd organ failure (hours), median (IQR)'}
        for n, g in grps.items():
            row[n] = summarize_median_iqr(g, 'lactate_to_second_organ_hours') if n in lactate_group_names else 'N/A'
        rws.append(row)

        # Lactate to 2nd organ failure mean
        row = {'Variable': 'Lactate to 2nd organ failure (hours), mean (SD)'}
        for n, g in grps.items():
            if n in lactate_group_names:
                vals = g['lactate_to_second_organ_hours'].dropna()
                row[n] = f"{vals.mean():.1f} ({vals.std():.1f})" if len(vals) > 0 else 'N/A'
            else:
                row[n] = 'N/A'
        rws.append(row)

        # Non-lactate 1st to 2nd organ failure median
        row = {'Variable': 'Non-lactate 1st to 2nd organ failure (hours), median (IQR)'}
        for n, g in grps.items():
            row[n] = summarize_median_iqr(g, 'non_lactate_to_second_organ_hours') if n in ase_group_names else 'N/A'
        rws.append(row)

        # Non-lactate 1st to 2nd organ failure mean
        row = {'Variable': 'Non-lactate 1st to 2nd organ failure (hours), mean (SD)'}
        for n, g in grps.items():
            if n in ase_group_names:
                vals = g['non_lactate_to_second_organ_hours'].dropna()
                row[n] = f"{vals.mean():.1f} ({vals.std():.1f})" if len(vals) > 0 else 'N/A'
            else:
                row[n] = 'N/A'
        rws.append(row)

        # First Organ Dysfunction (ASE groups only)
        rws.append({'Variable': '--- First Organ Dysfunction ---', **{n: '' for n in grps}})

        def summarize_first_organ_dysfunction(df, col):
            if col not in df.columns:
                return "N/A"
            vals = df[col].dropna()
            if len(vals) == 0:
                return "N/A"
            counts = vals.value_counts()
            if len(counts) == 0:
                return "N/A"
            top = counts.index[0]
            cnt = counts.iloc[0]
            total = len(vals)
            return f"{top}: {cnt} ({100*cnt/total:.1f}%)"

        row = {'Variable': 'First organ dysfunction (w/ lactate), top'}
        for n, g in grps.items():
            row[n] = summarize_first_organ_dysfunction(g, 'first_organ_dysfunction_w_lactate') if n in ase_group_names else 'N/A'
        rws.append(row)

        for val, lbl in [('vasopressor', 'Vasopressor'), ('imv', 'IMV'), ('aki', 'AKI'),
                         ('hyperbilirubinemia', 'Hyperbilirubinemia'), ('thrombocytopenia', 'Thrombocytopenia'),
                         ('lactate', 'Lactate')]:
            row = {'Variable': f'  {lbl}, n (%)'}
            for n, g in grps.items():
                row[n] = summarize_binary(g, g['first_organ_dysfunction_w_lactate'] == val) if n in ase_group_names else 'N/A'
            rws.append(row)

        row = {'Variable': 'First organ dysfunction (w/o lactate), top'}
        for n, g in grps.items():
            row[n] = summarize_first_organ_dysfunction(g, 'first_organ_dysfunction_wo_lactate') if n in ase_group_names else 'N/A'
        rws.append(row)

        for val, lbl in [('vasopressor', 'Vasopressor'), ('imv', 'IMV'), ('aki', 'AKI'),
                         ('hyperbilirubinemia', 'Hyperbilirubinemia'), ('thrombocytopenia', 'Thrombocytopenia')]:
            row = {'Variable': f'  {lbl}, n (%)'}
            for n, g in grps.items():
                row[n] = summarize_binary(g, g['first_organ_dysfunction_wo_lactate'] == val) if n in ase_group_names else 'N/A'
            rws.append(row)

        row = {'Variable': 'ASE onset criteria (w/ lactate), top'}
        for n, g in grps.items():
            row[n] = summarize_first_organ_dysfunction(g, 'ase_first_criteria_w_lactate') if n in ase_group_names else 'N/A'
        rws.append(row)

        for val, lbl in [('blood_culture', 'Blood Culture'), ('first_qad', 'First QAD'),
                         ('vasopressor', 'Vasopressor'), ('imv', 'IMV'), ('aki', 'AKI'),
                         ('hyperbilirubinemia', 'Hyperbilirubinemia'), ('thrombocytopenia', 'Thrombocytopenia'),
                         ('lactate', 'Lactate')]:
            row = {'Variable': f'  {lbl}, n (%)'}
            for n, g in grps.items():
                row[n] = summarize_binary(g, g['ase_first_criteria_w_lactate'] == val) if n in ase_group_names else 'N/A'
            rws.append(row)

        row = {'Variable': 'ASE onset criteria (w/o lactate), top'}
        for n, g in grps.items():
            row[n] = summarize_first_organ_dysfunction(g, 'ase_first_criteria_wo_lactate') if n in ase_group_names else 'N/A'
        rws.append(row)

        for val, lbl in [('blood_culture', 'Blood Culture'), ('first_qad', 'First QAD'),
                         ('vasopressor', 'Vasopressor'), ('imv', 'IMV'), ('aki', 'AKI'),
                         ('hyperbilirubinemia', 'Hyperbilirubinemia'), ('thrombocytopenia', 'Thrombocytopenia')]:
            row = {'Variable': f'  {lbl}, n (%)'}
            for n, g in grps.items():
                row[n] = summarize_binary(g, g['ase_first_criteria_wo_lactate'] == val) if n in ase_group_names else 'N/A'
            rws.append(row)

        # Outcomes
        rws.append({'Variable': '--- Outcomes ---', **{n: '' for n in grps}})
        rws.append({'Variable': 'Hospital LOS, mean (SD)', **{n: summarize_continuous(g, 'hospital_los_days') for n, g in grps.items()}})
        rws.append({
            'Variable': 'ICU LOS, mean (SD)',
            **{n: summarize_continuous(g[g['had_icu']], 'icu_los_days') if g['had_icu'].sum() > 0 else 'N/A' for n, g in grps.items()}
        })
        rws.append({'Variable': 'In-hospital death, n (%)', **{n: summarize_binary(g, 'in_hospital_death') for n, g in grps.items()}})

        # Microbiology
        rws.append({'Variable': '--- Microbiology ---', **{n: '' for n in grps}})

        # Index BC positivity - only for groups with presumed infection
        pi_groups = {'Presumed Infection', 'ASE with Lactate', 'ASE without Lactate', 'Lactate-only ASE'}
        row = {'Variable': 'Positive index BC, n (%)'}
        for n, g in grps.items():
            if n in pi_groups:
                row[n] = summarize_binary(g, 'index_bc_positive')
            else:
                row[n] = 'N/A'
        rws.append(row)

        # BCs in index window - only for groups with presumed infection
        row = {'Variable': 'BCs in index window, mean (SD)'}
        for n, g in grps.items():
            if n in pi_groups:
                row[n] = summarize_continuous(g, 'bc_count_in_window')
            else:
                row[n] = 'N/A'
        rws.append(row)

        # Positive BCs in window - only for groups with presumed infection
        row = {'Variable': 'Positive BCs in window, mean (SD)'}
        for n, g in grps.items():
            if n in pi_groups:
                row[n] = summarize_continuous(g, 'bc_positive_count_in_window')
            else:
                row[n] = 'N/A'
        rws.append(row)

        row = {'Variable': 'Positive BCs in window, median (IQR)'}
        for n, g in grps.items():
            if n in pi_groups:
                row[n] = summarize_median_iqr(g, 'bc_positive_count_in_window')
            else:
                row[n] = 'N/A'
        rws.append(row)

        rws.append({
            'Variable': 'Time to blood culture (hours), median (IQR)',
            **{n: summarize_median_iqr(g, 'time_to_bc_hours') for n, g in grps.items()}
        })

        return pd.DataFrame(rws), grps

    # Create and save stratified tables
    strat_results = {}
    strat_groups = {}
    onset_types = ['community', 'hospital']
    for ot in onset_types:
        result = build_stratified_table(analysis_df, ot)
        if result is not None:
            strat_results[ot] = result[0]
            strat_groups[ot] = result[1]

    saved_strat_paths = []
    if strat_results.get('community') is not None:
        comm_path = OUTPUT_DIR / f"{SITE_NAME}_table1_community_onset.csv"
        strat_results['community'].to_csv(comm_path, index=False)
        saved_strat_paths.append(str(comm_path))
        comm_json = compute_table1_json(strat_groups['community'], SITE_NAME, "community_onset")
        comm_json_path = OUTPUT_DIR / f"{SITE_NAME}_table1_community_onset.json"
        comm_json_path.write_text(json.dumps(comm_json, indent=2))
        print(f"Community-onset Table 1 saved: {comm_path}")
        print(f"Community-onset Table 1 JSON saved: {comm_json_path}")
        print(f"  N = {len(analysis_df[analysis_df['type'] == 'community']):,}")
    else:
        print("No community-onset cases found")

    if strat_results.get('hospital') is not None:
        hosp_path = OUTPUT_DIR / f"{SITE_NAME}_table1_hospital_onset.csv"
        strat_results['hospital'].to_csv(hosp_path, index=False)
        saved_strat_paths.append(str(hosp_path))
        hosp_json = compute_table1_json(strat_groups['hospital'], SITE_NAME, "hospital_onset")
        hosp_json_path = OUTPUT_DIR / f"{SITE_NAME}_table1_hospital_onset.json"
        hosp_json_path.write_text(json.dumps(hosp_json, indent=2))
        print(f"Hospital-onset Table 1 saved: {hosp_path}")
        print(f"Hospital-onset Table 1 JSON saved: {hosp_json_path}")
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
    print(f"No presumed infection: {analysis_df['group_no_presumed_infection'].sum():,}")
    print(f"Presumed infection: {analysis_df['group_presumed_infection'].sum():,}")
    print(f"ASE with lactate: {analysis_df['group_ase_w_lactate'].sum():,}")
    print(f"ASE without lactate: {analysis_df['group_ase_wo_lactate'].sum():,}")
    print(f"Lactate-only ASE: {analysis_df['group_lactate_only_ase'].sum():,}")
    if 'type' in analysis_df.columns:
        print(f"\nBy onset type:")
        print(f"  Community-onset: {(analysis_df['type'] == 'community').sum():,}")
        print(f"  Hospital-onset: {(analysis_df['type'] == 'hospital').sum():,}")
    return


if __name__ == "__main__":
    app.run()
