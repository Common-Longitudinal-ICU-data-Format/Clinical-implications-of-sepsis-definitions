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
    # Sepsis Definitions Cohort Identification

    This notebook builds the cohort for the sepsis definitions comparison study (ASE with/without lactate vs BSE).

    ## Encounter stitching
    Related hospitalizations of the same patient within a 6-hour window
    (discharge -> next admission) are stitched into a single **encounter_block**
    (clifpy `stitch_encounters`). The stitched encounter becomes the unit of
    analysis everywhere; its id is stored in the `hospitalization_id` column as
    `EB<n>`.

    ## Inclusion Criteria (applied per stitched encounter)
    - Adult hospitalized patients aged 18 years or older
    - Admitted 2018-2024
    - Admitted via the ED
    - Admitted to academic or community hospitals (excluding LTACH)
    - Must have an ED location during the encounter
    - Must have an ED **and** a ward or ICU location during the encounter
      (i.e. a true ED -> inpatient admission, not an ED-only visit)
    """)
    return


@app.cell
def _():
    import sys
    import json
    import pandas as pd
    import polars as pl
    from pathlib import Path

    # Make the local stitch_utils helper importable when run as `python Code/01_cohort.py`
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import stitch_utils as su

    from clifpy.tables import Patient, Hospitalization, Adt, Labs, HospitalDiagnosis
    from clifpy.utils.comorbidity import calculate_cci
    from clifpy.utils.sofa_polars import compute_sofa_polars
    from clifpy.utils.stitching_encounters import stitch_encounters
    return (
        Adt,
        HospitalDiagnosis,
        Hospitalization,
        Labs,
        Path,
        Patient,
        calculate_cci,
        compute_sofa_polars,
        json,
        pd,
        pl,
        stitch_encounters,
        su,
    )


@app.cell
def _(Path, json):
    # Load configuration (run notebook from project root)
    config_path = Path("clif_config.json")
    with open(config_path, "r") as f:
        config = json.load(f)

    DATA_DIR = config["data_directory"]
    FILETYPE = config["filetype"]
    TIMEZONE = config["timezone"]
    OUTPUT_DIR = Path(config["output_directory"])
    PHI_DIR = Path(config["phi_directory"])
    SITE_NAME = config["site_name"]
    STITCH_HOURS = int(config.get("stitch_time_interval_hours", 6))

    # Create both output directories
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PHI_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Site: {SITE_NAME}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Filetype: {FILETYPE}")
    print(f"Timezone: {TIMEZONE}")
    print(f"Stitch window: {STITCH_HOURS}h")
    return DATA_DIR, FILETYPE, OUTPUT_DIR, PHI_DIR, STITCH_HOURS, TIMEZONE


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 1: Load Core Tables
    """)
    return


@app.cell
def _(Adt, DATA_DIR, FILETYPE, Hospitalization, Patient, TIMEZONE):
    # Load patient, hospitalization, and adt tables
    patient = Patient.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE
    )

    hospitalization = Hospitalization.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE
    )

    adt = Adt.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE
    )

    print(f"Patient: {len(patient.df):,} rows")
    print(f"Hospitalization: {len(hospitalization.df):,} rows")
    print(f"ADT: {len(adt.df):,} rows")
    return adt, hospitalization, patient


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 1b: Stitch Encounters (6-hour window)

    Link hospitalizations of the same patient whose gap (prior discharge ->
    next admission) is <= 6 hours into a single `encounter_block`. We then build
    a block-level hospitalization table (one row per encounter) and a stitched
    ADT (events re-keyed to the encounter id). The mapping is saved for the
    downstream notebooks.
    """)
    return


@app.cell
def _(PHI_DIR, STITCH_HOURS, adt, hospitalization, stitch_encounters, su):
    # Stitch encounters on the FULL hospitalization + adt tables.
    _hosp_stitched, _adt_stitched, encounter_mapping = stitch_encounters(
        hospitalization.df,
        adt.df,
        time_interval=STITCH_HOURS,
    )

    # Persist the mapping (hospitalization_id -> encounter_block) for notebooks 2/3
    encounter_mapping.to_parquet(PHI_DIR / "encounter_mapping.parquet", index=False)

    # Block-level hospitalization (one row per encounter_block, ids => EB<n>)
    block_hosp = su.build_block_hospitalization(hospitalization.df, encounter_mapping)

    # ADT re-keyed to the stitched encounter id (all rows kept)
    adt_stitched = su.remap_ids(adt.df, encounter_mapping)

    _n_hosp = encounter_mapping["hospitalization_id"].nunique()
    _n_block = encounter_mapping["encounter_block"].nunique()
    print(f"Stitched {_n_hosp:,} hospitalizations -> {_n_block:,} encounter blocks")
    print(f"  Collapse ratio: {_n_hosp / _n_block:.3f} hospitalizations per encounter")
    _per_block = encounter_mapping.groupby("encounter_block").size()
    print(f"  Encounters with >1 hospitalization: {(_per_block > 1).sum():,}")
    print(f"  Max hospitalizations in one encounter: {_per_block.max()}")
    return adt_stitched, block_hosp, encounter_mapping


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 2: Apply Inclusion Criteria
    """)
    return


@app.cell
def _(adt_stitched, block_hosp):
    # Work at the stitched-encounter level: block_hosp has one row per
    # encounter_block (hospitalization_id = EB<n>). Attach the encounter's
    # admitting hospital = the hospital of the earliest ADT location (index).
    hosp_lookup = (
        adt_stitched.sort_values("in_dttm")
        .drop_duplicates("hospitalization_id", keep="first")
        [["hospitalization_id", "hospital_id", "hospital_type"]]
    )
    merged_df = block_hosp.merge(hosp_lookup, on="hospitalization_id", how="inner")

    print(f"Total encounters after merge with ADT: {merged_df['hospitalization_id'].nunique():,}")
    return (merged_df,)


@app.cell
def _(merged_df):
    # Filter 1: Adults (age >= 18)
    cohort_df = merged_df[
        (merged_df["age_at_admission"] >= 18) &
        (merged_df["age_at_admission"].notna())
    ].copy()

    print(f"After age filter (>=18): {cohort_df['hospitalization_id'].nunique():,} hospitalizations")
    return (cohort_df,)


@app.cell
def _(cohort_df):
    # Filter 2: Date range 2018-2024 + non-null dates
    cohort_df_filtered = cohort_df[
        (cohort_df["admission_dttm"].notna()) &
        (cohort_df["discharge_dttm"].notna()) &
        (cohort_df["admission_dttm"].dt.year >= 2018) &
        (cohort_df["admission_dttm"].dt.year <= 2024) &
        (cohort_df["discharge_dttm"].dt.year >= 2018) &
        (cohort_df["discharge_dttm"].dt.year <= 2024)
    ].copy()

    print(f"After date filter (2018-2024): {cohort_df_filtered['hospitalization_id'].nunique():,} hospitalizations")
    return (cohort_df_filtered,)


@app.cell
def _(cohort_df_filtered):
    # Filter 3: Admitted via ED
    cohort_df_ed = cohort_df_filtered[
        cohort_df_filtered["admission_type_category"].str.lower() == "ed"
    ].copy()

    print(f"After ED admission filter: {cohort_df_ed['hospitalization_id'].nunique():,} hospitalizations")
    return (cohort_df_ed,)


@app.cell
def _(cohort_df_ed):
    # Filter 4: Academic or community hospitals (exclude LTACH)
    cohort_df_hospital = cohort_df_ed[
        cohort_df_ed["hospital_type"].isin(["academic", "community"])
    ].copy()

    print(f"After hospital type filter (academic/community): {cohort_df_hospital['hospitalization_id'].nunique():,} hospitalizations")
    return (cohort_df_hospital,)


@app.cell
def _(adt_stitched, cohort_df_hospital):
    # Filter 5: Must have an ED location during the encounter (stitched ADT)
    _loc = adt_stitched["location_category"].str.lower()
    hosp_with_ed = set(adt_stitched.loc[_loc == "ed", "hospitalization_id"].unique())

    cohort_df_ed_loc = cohort_df_hospital[
        cohort_df_hospital["hospitalization_id"].isin(hosp_with_ed)
    ].copy()

    print(f"After ED location filter: {cohort_df_ed_loc['hospitalization_id'].nunique():,} encounters")
    return (cohort_df_ed_loc,)


@app.cell
def _(adt_stitched, cohort_df_ed_loc):
    # Filter 6 (NEW): Must also have a ward or ICU location during the encounter,
    # i.e. a true ED -> inpatient admission (drops ED-only visits sent home).
    _loc = adt_stitched["location_category"].str.lower()
    hosp_with_ward_icu = set(
        adt_stitched.loc[_loc.isin(["ward", "icu"]), "hospitalization_id"].unique()
    )

    cohort_df_final = cohort_df_ed_loc[
        cohort_df_ed_loc["hospitalization_id"].isin(hosp_with_ward_icu)
    ].copy()

    print(
        f"After ED + (ward or ICU) filter: "
        f"{cohort_df_final['hospitalization_id'].nunique():,} encounters"
    )
    return (cohort_df_final,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 3: Build Final Cohort with Demographics
    """)
    return


@app.cell
def _(cohort_df_final, patient, pd):
    # Build final cohort with one row per hospitalization
    final_cohort = cohort_df_final.drop_duplicates(subset=["hospitalization_id"]).copy()

    # Add patient demographics
    patient_df = patient.df[["patient_id", "death_dttm", "race_category", "sex_category", "ethnicity_category"]]
    final_cohort = pd.merge(final_cohort, patient_df, on="patient_id", how="left")

    print("Final Cohort Summary:")
    print(f"  Hospitalizations: {final_cohort['hospitalization_id'].nunique():,}")
    print(f"  Unique patients: {final_cohort['patient_id'].nunique():,}")

    # Create combined race/ethnicity column
    def get_race_ethnicity(row):
        race = row["race_category"].lower() if isinstance(row["race_category"], str) else row["race_category"]
        eth = row["ethnicity_category"].lower() if isinstance(row["ethnicity_category"], str) else row["ethnicity_category"]
        if race == "asian":
            return "Asian"
        elif eth == "hispanic" and race == "black or african american":
            return "Hispanic Black"
        elif eth == "hispanic" and race == "white":
            return "Hispanic White"
        elif eth == "non-hispanic" and race == "black or african american":
            return "Non-Hispanic Black"
        elif eth == "non-hispanic" and race == "white":
            return "Non-Hispanic White"
        else:
            return "Other"

    final_cohort["race_ethnicity"] = final_cohort.apply(get_race_ethnicity, axis=1)

    print("\nRace/Ethnicity distribution:")
    print(final_cohort["race_ethnicity"].value_counts())
    return (final_cohort,)


@app.cell
def _(final_cohort):
    # Display cohort demographics
    print("Demographics:")
    print(f"  Age: mean={final_cohort['age_at_admission'].mean():.1f}, median={final_cohort['age_at_admission'].median():.1f}")
    print("Sex distribution:")
    print(final_cohort["sex_category"].value_counts())
    print("Race/Ethnicity distribution:")
    print(final_cohort["race_ethnicity"].value_counts())
    print("Hospital type distribution:")
    print(final_cohort["hospital_type"].value_counts())
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 4: Save Cohort to Parquet
    """)
    return


@app.cell
def _(PHI_DIR, final_cohort):
    # Save cohort (PHI - patient-level data)
    cohort_output_path = PHI_DIR / "cohort_df.parquet"
    final_cohort.to_parquet(cohort_output_path, index=False)

    print(f"Cohort saved to: {cohort_output_path}")
    print(f"Shape: {final_cohort.shape}")
    return


@app.cell
def _(final_cohort):
    # Display final cohort
    final_cohort.head(10)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 4b: CONSORT Diagram

    Track the cohort count from all hospitalizations, through the stitch
    collapse to encounter blocks, then each inclusion/exclusion filter. Counts
    are saved to `consort_counts.json` and rendered to `consort_diagram.png`
    (both shareable aggregate outputs, with <11 cell suppression).
    """)
    return


@app.cell
def _(
    OUTPUT_DIR,
    block_hosp,
    cohort_df,
    cohort_df_ed,
    cohort_df_ed_loc,
    cohort_df_filtered,
    cohort_df_final,
    cohort_df_hospital,
    encounter_mapping,
    final_cohort,
    hospitalization,
    merged_df,
    su,
):
    # Reverse lookup: stitched encounter id -> member hospitalization ids
    _map = encounter_mapping.copy()
    _map["eb"] = _map["encounter_block"].map(su.encounter_id)

    def _nhosp(df):
        ebs = set(df["hospitalization_id"].unique())
        return int(_map.loc[_map["eb"].isin(ebs), "hospitalization_id"].nunique())

    def _row(step, df, n_blocks=None, n_hosp=None, n_pat=None):
        if df is not None:
            n_blocks = df["hospitalization_id"].nunique()
            n_pat = df["patient_id"].nunique() if "patient_id" in df.columns else None
            n_hosp = _nhosp(df)
        return {
            "step": step,
            "n_encounter_blocks": None if n_blocks is None else int(n_blocks),
            "n_hospitalizations": None if n_hosp is None else int(n_hosp),
            "n_patients": None if n_pat is None else int(n_pat),
        }

    consort_steps = [
        # Raw hospitalizations (pre-stitch): no encounter-block count yet
        _row(
            "all_hospitalizations", None,
            n_hosp=hospitalization.df["hospitalization_id"].nunique(),
            n_pat=hospitalization.df["patient_id"].nunique(),
        ),
        _row("after_stitch_6h", block_hosp),
        _row("linked_to_adt", merged_df),
        _row("age_ge_18", cohort_df),
        _row("admitted_2018_2024", cohort_df_filtered),
        _row("admitted_via_ed", cohort_df_ed),
        _row("academic_or_community", cohort_df_hospital),
        _row("has_ed_location", cohort_df_ed_loc),
        _row("has_ed_and_ward_or_icu", cohort_df_final),
        _row("final_cohort", final_cohort),
    ]

    # n_excluded = drop in encounter blocks vs the previous block-counted step
    _prev = None
    for _s in consort_steps:
        _nb = _s["n_encounter_blocks"]
        _s["n_excluded"] = int(_prev - _nb) if (_prev is not None and _nb is not None) else None
        if _nb is not None:
            _prev = _nb

    su.save_consort(consort_steps, OUTPUT_DIR / "consort_counts.json")
    _fig = su.plot_consort(consort_steps, title="Sepsis cohort CONSORT (6h-stitched encounters)")
    _fig.savefig(OUTPUT_DIR / "consort_diagram.png", dpi=150, bbox_inches="tight")

    print("CONSORT flow (encounter blocks):")
    for _s in consort_steps:
        print(
            f"  {_s['step']:>24}: blocks={_s['n_encounter_blocks']}, "
            f"hosps={_s['n_hospitalizations']}, excluded={_s['n_excluded']}"
        )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 4c: Materialize Stitched CLIF Tables

    Write a copy of every CLIF table the pipeline consumes into
    `PHI_DIR/intermediate/stitched_tables/`, with `hospitalization_id`
    overwritten by the stitched encounter id. Downstream notebooks point clifpy
    at this derived directory (computed from `phi_directory` via
    `su.stitched_dir`) so ASE, SOFA, CCI and the R analysis operate natively at
    the encounter level — all driven from a single `clif_config.json`.
    """)
    return


@app.cell
def _(
    DATA_DIR,
    FILETYPE,
    PHI_DIR,
    TIMEZONE,
    block_hosp,
    encounter_mapping,
    final_cohort,
    su,
):
    # Resolve the cohort's original member hospitalization ids and patients
    _final_ebs = set(final_cohort["hospitalization_id"].unique())
    _map = encounter_mapping.copy()
    _map["eb"] = _map["encounter_block"].map(su.encounter_id)
    _member = _map[_map["eb"].isin(_final_ebs)]
    member_hosp_ids = _member["hospitalization_id"].astype(str).unique().tolist()
    patient_ids = final_cohort["patient_id"].astype(str).unique().tolist()
    block_hosp_final = block_hosp[block_hosp["hospitalization_id"].isin(_final_ebs)].copy()

    su.materialize_stitched_tables(
        base_data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE,
        phi_dir=PHI_DIR,
        mapping=encounter_mapping,
        member_hosp_ids=member_hosp_ids,
        patient_ids=patient_ids,
        block_hospitalization=block_hosp_final,
        verbose=True,
    )

    STITCHED_DIR = str(su.stitched_dir(PHI_DIR))
    print(f"Stitched tables -> {STITCHED_DIR}")
    return (STITCHED_DIR,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 5: Calculate Adult Sepsis Event (ASE)
    """)
    return


@app.cell
def _():
    from clifpy.utils.ase import compute_ase
    return (compute_ase,)


@app.cell
def _(FILETYPE, STITCHED_DIR, TIMEZONE, compute_ase, final_cohort):
    # Get stitched-encounter IDs from cohort (stored in hospitalization_id)
    hosp_ids = final_cohort["hospitalization_id"].astype(str).unique().tolist()
    print(f"Running ASE calculation on {len(hosp_ids):,} stitched encounters...")

    # Calculate ASE using clifpy (returns ALL blood cultures, both ASE and non-ASE).
    # Runs against the STITCHED directory so blood cultures, QAD antibiotics and
    # organ dysfunction from all member hospitalizations are grouped under one
    # encounter id. include_lactate=True is REQUIRED to compare definitions:
    # - sepsis: ASE with lactate as organ dysfunction criterion
    # - sepsis_wo_lactate: ASE without lactate criterion
    ase_results_all = compute_ase(
        hospitalization_ids=hosp_ids,
        data_directory=STITCHED_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE,
        apply_rit=True,
        rit_only_hospital_onset=True,
        include_lactate=True,  # Must be True to compare lactate vs non-lactate
        verbose=True
    )

    # Keep first ASE episode OR first blood culture (for non-ASE patients)
    # Sort: episode_id==1 rows first (notna sorts before NA), then earliest BC
    ase_sorted = ase_results_all.sort_values(
        ["hospitalization_id", "episode_id", "blood_culture_dttm"],
        na_position="last",
    )

    # One row per hospitalization: prefer episode_id==1, else first BC
    ase_results = ase_sorted.drop_duplicates(
        subset=["hospitalization_id"], keep="first"
    ).copy()

    # QC: flag duplicates
    n_dupes = ase_results["hospitalization_id"].duplicated().sum()
    if n_dupes > 0:
        print(f"WARNING: {n_dupes} duplicate hospitalization_ids in ase_results!")
    else:
        print("QC passed: no duplicate hospitalization_ids")

    n_ase = (ase_results["episode_id"] == 1).sum()
    n_non_ase = ase_results["episode_id"].isna().sum()
    print(f"Filtered to first episode/BC: {len(ase_results):,} hospitalizations")
    print(f"  - ASE cases (episode_id==1): {n_ase:,}")
    print(f"  - Non-ASE with blood culture: {n_non_ase:,}")
    return (ase_results,)


@app.cell
def _(ase_results):
    # Display ASE summary
    print("ASE Results Summary:")
    print(f"  Total blood cultures evaluated: {len(ase_results):,}")
    print(f"  Presumed infection: {ase_results['presumed_infection'].sum():,}")
    print(f"  ASE with lactate (sepsis): {ase_results['sepsis'].sum():,}")
    print(f"  ASE without lactate: {ase_results['sepsis_wo_lactate'].sum():,}")
    if "type" in ase_results.columns:
        ase_only = ase_results[ase_results['sepsis'] == 1]
        print(f"  Community-onset ASE: {(ase_only['type'] == 'community').sum():,}")
        print(f"  Hospital-onset ASE: {(ase_only['type'] == 'hospital').sum():,}")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 6: Save ASE Results to Parquet
    """)
    return


@app.cell
def _(PHI_DIR, ase_results):
    # Save ASE results (PHI - patient-level data)
    ase_output_path = PHI_DIR / "ase_results.parquet"
    ase_results.to_parquet(ase_output_path, index=False)

    print(f"ASE results saved to: {ase_output_path}")
    print(f"Shape: {ase_results.shape}")
    return


@app.cell
def _(OUTPUT_DIR, ase_results, final_cohort, pd):
    # Organ dysfunction breakdown across 3 ASE groups
    organ_cols = {
        'vasopressor': 'vasopressor_dttm',
        'IMV': 'imv_dttm',
        'AKI': 'aki_dttm',
        'bilirubin': 'hyperbilirubinemia_dttm',
        'thrombocytopenia': 'thrombocytopenia_dttm',
        'lactate': 'lactate_dttm',
    }
    organ_cols_no_lactate = {k: v for k, v in organ_cols.items() if k != 'lactate'}

    # Merge patient_id from final_cohort (local to this cell)
    ase_with_pid = ase_results.merge(
        final_cohort[["hospitalization_id", "patient_id"]].drop_duplicates(),
        on="hospitalization_id", how="left"
    )

    # Group 1: ASE with lactate criterion AND actually had elevated lactate
    group1 = ase_with_pid[(ase_with_pid['sepsis'] == 1) & (ase_with_pid['lactate_dttm'].notna())]
    # Group 2: All ASE (6-criterion definition)
    group2 = ase_with_pid[ase_with_pid['sepsis'] == 1]
    # Group 3: ASE without lactate criterion (5-criterion definition)
    group3 = ase_with_pid[ase_with_pid['sepsis_wo_lactate'] == 1]

    groups = {
        'ASE_with_lactate_AND_met_lactate': group1,
        'ASE_with_lactate': group2,
        'ASE_without_lactate': group3,
    }

    rows = {}
    for gname, gdf in groups.items():
        n_total = len(gdf)
        rows.setdefault('n_hospitalizations', {})[gname] = gdf['hospitalization_id'].nunique()
        rows.setdefault('n_patients', {})[gname] = gdf['patient_id'].nunique()
        # Per-organ counts and percentages
        for organ, col in organ_cols.items():
            n = int(gdf[col].notna().sum())
            pct = round(n / n_total * 100, 1) if n_total > 0 else 0.0
            rows.setdefault(f'{organ}_n', {})[gname] = n
            rows.setdefault(f'{organ}_percent', {})[gname] = pct

        # Organ failure totals — group 3 uses 5 criteria (no lactate)
        if gname == 'ASE_without_lactate':
            count_cols = list(organ_cols_no_lactate.values())
            max_organs = 5
        else:
            count_cols = list(organ_cols.values())
            max_organs = 6

        organ_count = gdf[count_cols].notna().sum(axis=1)
        count_dist = organ_count.value_counts()
        for i in range(1, 7):
            key = f'total {i} organ failure{"s" if i > 1 else ""}_n'
            if gname == 'ASE_without_lactate' and i > max_organs:
                rows.setdefault(key, {})[gname] = 'NA'
            else:
                rows.setdefault(key, {})[gname] = int(count_dist.get(i, 0))

    breakdown_df = pd.DataFrame(rows).T
    breakdown_df.index.name = 'metric'

    out_path = OUTPUT_DIR / "organ_dysfunction_breakdown.csv"
    breakdown_df.to_csv(out_path)
    print(f"Organ dysfunction breakdown saved to: {out_path}")
    print(f"Shape: {breakdown_df.shape}")
    print(breakdown_df.to_string())
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 8: Lactate Orders per 1,000 Patient-Days by Hospital
    """)
    return


@app.cell
def _(
    DATA_DIR,
    FILETYPE,
    Labs,
    OUTPUT_DIR,
    TIMEZONE,
    adt,
    encounter_mapping,
    hospitalization,
    su,
):
    # NOTE: this hospital-level utilization rate is computed over ALL
    # hospitalizations (the full denominator), so it deliberately uses the
    # original (un-stitched) tables. Only the per-encounter `lactate_counts`
    # returned below is re-keyed to the stitched encounter id for the cohort.

    # --- 1. All hospitalizations (unfiltered) with LOS ---
    all_hosp = hospitalization.df[["hospitalization_id", "admission_dttm", "discharge_dttm"]].copy()
    all_hosp = all_hosp.dropna(subset=["admission_dttm", "discharge_dttm"])
    all_hosp = all_hosp[
        (all_hosp["admission_dttm"].dt.year >= 2018) &
        (all_hosp["admission_dttm"].dt.year <= 2024)
    ]
    all_hosp["los_days"] = (
        (all_hosp["discharge_dttm"] - all_hosp["admission_dttm"])
        .dt.total_seconds() / 86400
    )
    all_hosp["los_days"] = all_hosp["los_days"].clip(lower=1)
    all_hosp["admission_year"] = all_hosp["admission_dttm"].dt.year

    # --- 2. Map hospital_id / hospital_type from ADT (first record per hosp) ---
    adt_hosp = (
        adt.df[["hospitalization_id", "hospital_id", "hospital_type"]]
        .drop_duplicates(subset=["hospitalization_id"], keep="first")
    )
    all_hosp = all_hosp.merge(adt_hosp, on="hospitalization_id", how="inner")

    # --- 3. Load ALL lactate lab orders ---
    lactate_labs = Labs.from_file(
        data_directory=DATA_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE,
        filters={"lab_category": ["lactate"]},
    )
    # Original-id counts drive the hospital-wide rate (denominator = all hosps)
    lactate_counts_orig = (
        lactate_labs.df
        .groupby("hospitalization_id")
        .size()
        .reset_index(name="lactate_orders")
    )

    # Stitched-encounter counts drive the per-encounter any_lactate_ordered flag
    _lac_eb = lactate_labs.df.merge(encounter_mapping, on="hospitalization_id", how="inner")
    _lac_eb["hospitalization_id"] = _lac_eb["encounter_block"].map(su.encounter_id)
    lactate_counts = (
        _lac_eb.groupby("hospitalization_id").size().reset_index(name="lactate_orders")
    )

    # --- 4. Join lactate counts onto hospitalizations (rate uses original ids) ---
    all_hosp = all_hosp.merge(lactate_counts_orig, on="hospitalization_id", how="left")
    all_hosp["lactate_orders"] = all_hosp["lactate_orders"].fillna(0).astype(int)

    # --- 5. Aggregate by hospital_id, hospital_type, admission_year ---
    lactate_rate = (
        all_hosp
        .groupby(["hospital_id", "hospital_type", "admission_year"])
        .agg(
            n_hospitalizations=("hospitalization_id", "count"),
            total_patient_days=("los_days", "sum"),
            total_lactate_orders=("lactate_orders", "sum"),
        )
        .reset_index()
    )
    lactate_rate["lactate_order_per_1000_patient_days"] = (
        lactate_rate["total_lactate_orders"] / lactate_rate["total_patient_days"]
    ) * 1000

    # --- 6. Save ---
    lactate_rate_path = OUTPUT_DIR / "lactate_orders_per_1000_patient_days.parquet"
    lactate_rate.to_parquet(lactate_rate_path, index=False)

    lactate_rate_csv_path = OUTPUT_DIR / "lactate_orders_per_1000_patient_days.csv"
    lactate_rate.to_csv(lactate_rate_csv_path, index=False)

    print(f"Lactate orders per 1,000 patient-days saved to: {lactate_rate_path}")
    print(f"Shape: {lactate_rate.shape}")
    print(lactate_rate.head(10))
    return lactate_counts, lactate_rate


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 8b: Monthly Hospitalization Counts
    """)
    return


@app.cell
def _(hospitalization, OUTPUT_DIR, pd):
    # Total hospitalizations per month (all, regardless of admission type or ASE)
    all_hosp_monthly = hospitalization.df[["hospitalization_id", "admission_dttm"]].copy()
    all_hosp_monthly = all_hosp_monthly.dropna(subset=["admission_dttm"])
    all_hosp_monthly = all_hosp_monthly[
        (all_hosp_monthly["admission_dttm"].dt.year >= 2018) &
        (all_hosp_monthly["admission_dttm"].dt.year <= 2024)
    ]
    all_hosp_monthly["month"] = all_hosp_monthly["admission_dttm"].dt.to_period("M").astype(str)

    month_counts = (
        all_hosp_monthly
        .groupby("month")["hospitalization_id"]
        .nunique()
        .reset_index(name="n_hospitalizations")
    )

    # Cell suppression: replace counts < 11 with -99
    month_counts.loc[month_counts["n_hospitalizations"] < 11, "n_hospitalizations"] = -99

    month_counts.to_csv(OUTPUT_DIR / "month_hospitalizations.csv", index=False)
    print(f"Monthly hospitalization counts saved to: {OUTPUT_DIR / 'month_hospitalizations.csv'}")
    print(f"Shape: {month_counts.shape}")
    print(month_counts.to_string(index=False))
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Step 9: Analysis Dataset (2 Rows per Hospitalization)
    """)
    return


@app.cell
def _(FILETYPE, HospitalDiagnosis, STITCHED_DIR, TIMEZONE, calculate_cci):
    # Read the STITCHED hospital_diagnosis so CCI is keyed by encounter id
    # (diagnoses from all member hospitalizations roll up to the encounter).
    hospital_diagnosis = HospitalDiagnosis.from_file(
        data_directory=STITCHED_DIR,
        filetype=FILETYPE,
        timezone=TIMEZONE,
    )
    cci_df = calculate_cci(hospital_diagnosis)
    print(f"CCI computed for {len(cci_df):,} encounters")
    print(f"CCI score distribution:\n{cci_df['cci_score'].describe()}")
    return (cci_df,)


@app.cell
def _(
    FILETYPE,
    STITCHED_DIR,
    TIMEZONE,
    compute_sofa_polars,
    final_cohort,
    pd,
    pl,
):
    # Build SOFA cohort with two window types per stitched encounter:
    # 1. 24-hour window (admission → admission + 24h) — standardized severity measure
    # 2. Full-encounter window (admission → discharge) — fallback
    #
    # The cohort's `hospitalization_id` already holds the stitched encounter id and
    # the STITCHED tables are keyed the same way, so compute_sofa_polars aggregates
    # labs/vitals across the whole encounter. `sofa_window_id` is a throwaway label
    # distinguishing the 24h vs full window for the SAME encounter (it is NOT the
    # stitched encounter id).

    # 24-hour SOFA (admission → admission + 24h)
    sofa_24h = final_cohort[["hospitalization_id", "admission_dttm"]].drop_duplicates(
        subset=["hospitalization_id"]
    ).copy()
    sofa_24h["sofa_window_id"] = sofa_24h["hospitalization_id"].astype(str) + "_24h"
    sofa_24h["end_dttm"] = sofa_24h["admission_dttm"] + pd.Timedelta(hours=24)
    sofa_24h = sofa_24h.rename(columns={"admission_dttm": "start_dttm"})

    # Full-encounter SOFA (admission → discharge, fallback)
    full_hosp = final_cohort[["hospitalization_id", "admission_dttm", "discharge_dttm"]].drop_duplicates(
        subset=["hospitalization_id"]
    )
    full_hosp["sofa_window_id"] = full_hosp["hospitalization_id"].astype(str) + "_full"
    full_hosp = full_hosp.rename(columns={"admission_dttm": "start_dttm", "discharge_dttm": "end_dttm"})

    # Combine and convert to polars for compute_sofa_polars
    sofa_cohort_pl = pl.from_pandas(
        pd.concat([sofa_24h, full_hosp], ignore_index=True)[
            ["hospitalization_id", "sofa_window_id", "start_dttm", "end_dttm"]
        ]
    )
    print(f"SOFA cohort: {len(sofa_cohort_pl):,} encounter-windows")
    print(f"  24-hour windows: {len(sofa_24h):,}")
    print(f"  Full-encounter windows: {len(full_hosp):,}")

    # Compute SOFA scores within each time window (grouped by sofa_window_id)
    sofa_result_pl = compute_sofa_polars(
        STITCHED_DIR,
        sofa_cohort_pl,
        filetype=FILETYPE,
        id_name="sofa_window_id",
        timezone=TIMEZONE,
    )

    # Split results by suffix, recovering the stitched encounter id
    sofa_24h_df = (
        sofa_result_pl
        .filter(pl.col("sofa_window_id").str.ends_with("_24h"))
        .with_columns(
            pl.col("sofa_window_id").str.replace("_24h$", "").alias("hospitalization_id")
        )
        .select(["hospitalization_id", "sofa_total"])
    )
    sofa_full_df = (
        sofa_result_pl
        .filter(pl.col("sofa_window_id").str.ends_with("_full"))
        .with_columns(
            pl.col("sofa_window_id").str.replace("_full$", "").alias("hospitalization_id")
        )
        .select(["hospitalization_id", "sofa_total"])
    )

    print(f"SOFA results: {len(sofa_24h_df):,} 24-hour, {len(sofa_full_df):,} full-encounter")
    return sofa_24h_df, sofa_full_df


@app.cell
def _(
    PHI_DIR,
    ase_results,
    cci_df,
    final_cohort,
    lactate_counts,
    lactate_rate,
    pd,
    sofa_24h_df,
    sofa_full_df,
):
    # --- 1. Stack into 2 rows per hospitalization ---
    melt_src = ase_results[["hospitalization_id", "sepsis", "sepsis_wo_lactate"]].copy()
    melt_src = melt_src.rename(columns={"sepsis": 1, "sepsis_wo_lactate": 0})
    analysis_df = melt_src.melt(
        id_vars=["hospitalization_id"],
        value_vars=[1, 0],
        var_name="defined_w_lactic",
        value_name="ASE",
    )

    # Sanity-check: count hospitalizations by (ASE_wl, ASE_wol) pattern
    _pattern = analysis_df.pivot_table(
        index="hospitalization_id", columns="defined_w_lactic", values="ASE", aggfunc="first"
    )
    _pattern_counts = _pattern.groupby([1, 0]).size().reset_index(name="n_hosp")
    print("ASE pattern counts (wl, wol):\n", _pattern_counts.to_string(index=False))
    assert (_pattern_counts.query("`1` == 0 and `0` == 1")["n_hosp"].sum() == 0), \
        "Impossible pattern found: ASE=0 with lactate but ASE=1 without lactate"

    # --- 2. Join demographics from final_cohort ---
    demo = final_cohort[
        ["hospitalization_id", "age_at_admission", "sex_category", "hospital_id", "hospital_type", "admission_dttm"]
    ].drop_duplicates(subset=["hospitalization_id"])
    analysis_df = analysis_df.merge(demo, on="hospitalization_id", how="left")
    analysis_df = analysis_df.rename(columns={"age_at_admission": "age", "sex_category": "sex"})

    # --- 3. onset_community1_hospital0 (hospitalization-level, same for both rows) ---
    # Build lookup from ase_results + admission_dttm (already in analysis_df via demo)
    onset_src = ase_results[["hospitalization_id"]].copy()
    onset_src["_type"] = ase_results["type"].values
    onset_src["_onset_wol_dttm"] = (
        ase_results["ase_onset_wo_lactate_dttm"].values
        if "ase_onset_wo_lactate_dttm" in ase_results.columns
        else pd.NaT
    )
    # admission_dttm comes from final_cohort (not in ase_results)
    onset_src = onset_src.merge(
        demo[["hospitalization_id", "admission_dttm"]],
        on="hospitalization_id",
        how="left",
    )
    onset_src = onset_src.drop_duplicates(subset=["hospitalization_id"])

    def _onset_type(row):
        t = row["_type"]
        if pd.notna(t):
            return 1 if t == "community" else 0
        # Fallback: compute from days since admission
        onset_dt = row["_onset_wol_dttm"]
        adm_dt = row["admission_dttm"]
        if pd.notna(onset_dt) and pd.notna(adm_dt):
            day = (onset_dt - adm_dt).days + 1
            return 1 if day <= 2 else 0
        return None

    onset_src["onset_community1_hospital0"] = onset_src.apply(_onset_type, axis=1)
    analysis_df = analysis_df.merge(
        onset_src[["hospitalization_id", "onset_community1_hospital0"]],
        on="hospitalization_id",
        how="left",
    )

    # --- 4. SOFA admission-to-24h (fixed window, same for both rows) ---
    sofa_24h_pd = sofa_24h_df.to_pandas()
    sofa_24h_map = sofa_24h_pd.set_index("hospitalization_id")["sofa_total"]
    analysis_df["worst_sofa_admission_to_24h"] = analysis_df["hospitalization_id"].map(sofa_24h_map)

    # Fallback: fill remaining NaN with full-hospitalization SOFA
    sofa_full_pd = sofa_full_df.to_pandas()
    sofa_full_map = sofa_full_pd.set_index("hospitalization_id")["sofa_total"]
    null_mask = analysis_df["worst_sofa_admission_to_24h"].isna()
    analysis_df.loc[null_mask, "worst_sofa_admission_to_24h"] = (
        analysis_df.loc[null_mask, "hospitalization_id"].map(sofa_full_map).values
    )

    # --- 5. CCI ---
    cci_merge = (
        cci_df.reset_index()[["hospitalization_id", "cci_score"]]
        if "hospitalization_id" not in cci_df.columns
        else cci_df[["hospitalization_id", "cci_score"]]
    )
    cci_merge = cci_merge.rename(columns={"cci_score": "cci"})
    analysis_df = analysis_df.merge(cci_merge, on="hospitalization_id", how="left")

    # --- 6. Presumed infection (hospitalization-level) ---
    pi_map = ase_results.drop_duplicates("hospitalization_id").set_index("hospitalization_id")["presumed_infection"]
    analysis_df["presumed_infection"] = analysis_df["hospitalization_id"].map(pi_map).fillna(0).astype(int)

    # --- 7. Any lactate ordered (hospitalization-level) ---
    lactate_flag = lactate_counts.set_index("hospitalization_id")["lactate_orders"].gt(0).astype(int)
    analysis_df["any_lactate_ordered"] = analysis_df["hospitalization_id"].map(lactate_flag).fillna(0).astype(int)

    # --- 8. Lactate ordering rate ---
    analysis_df["year"] = analysis_df["admission_dttm"].dt.year
    analysis_df = analysis_df.merge(
        lactate_rate[["hospital_id", "admission_year", "lactate_order_per_1000_patient_days"]].rename(
            columns={"admission_year": "year"}
        ),
        on=["hospital_id", "year"],
        how="left",
    )

    # --- 9. Hospitalizations from ED per year ---
    ed_hosp_per_year = (
        final_cohort
        .groupby(["hospital_id", final_cohort["admission_dttm"].dt.year])
        .size()
        .reset_index(name="hospitalizations_from_ed_per_year")
        .rename(columns={"admission_dttm": "year"})
    )
    analysis_df = analysis_df.merge(ed_hosp_per_year, on=["hospital_id", "year"], how="left")

    # --- 10. Select final columns & save ---
    final_cols = [
        "hospitalization_id",
        "ASE",
        "defined_w_lactic",
        "onset_community1_hospital0",
        "age",
        "sex",
        "cci",
        "worst_sofa_admission_to_24h",
        "any_lactate_ordered",
        "presumed_infection",
        "hospital_id",
        "hospital_type",
        "lactate_order_per_1000_patient_days",
        "year",
        "hospitalizations_from_ed_per_year",
    ]
    analysis_df = analysis_df[final_cols]

    analysis_path = PHI_DIR / "analysis_dataset.parquet"
    analysis_df.to_parquet(analysis_path, index=False)

    # Verification
    n_hosp = ase_results["hospitalization_id"].nunique()
    print(f"Analysis dataset saved to: {analysis_path}")
    print(f"Shape: {analysis_df.shape} (expected {n_hosp * 2} rows)")
    print(f"ASE=1 (wl=1): {analysis_df.loc[analysis_df['defined_w_lactic']==1, 'ASE'].sum():,}")
    print(f"ASE=1 (wl=0): {analysis_df.loc[analysis_df['defined_w_lactic']==0, 'ASE'].sum():,}")
    print(f"SOFA non-null: {analysis_df['worst_sofa_admission_to_24h'].notna().sum():,}")
    print(f"CCI non-null: {analysis_df['cci'].notna().sum():,}")
    print(f"any_lactate_ordered=1: {analysis_df['any_lactate_ordered'].sum():,}")
    print(f"presumed_infection=1: {analysis_df['presumed_infection'].sum():,}")
    print(f"Columns: {list(analysis_df.columns)}")
    print(f"hospital_type values: {analysis_df['hospital_type'].value_counts().to_dict()}")
    print(f"year range: {analysis_df['year'].min()}–{analysis_df['year'].max()}")
    print(f"hospitalizations_from_ed_per_year non-null: {analysis_df['hospitalizations_from_ed_per_year'].notna().sum():,}")
    return


if __name__ == "__main__":
    app.run()
