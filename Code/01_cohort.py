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

    ## Inclusion Criteria
    - Adult hospitalized patients aged 18 years or older
    - Admitted 2018-2024
    - Admitted via the ED
    - Admitted to academic or community hospitals (excluding LTACH)
    - Must have ED location during hospitalization
    """)
    return


@app.cell
def _():
    import json
    import pandas as pd
    from pathlib import Path
    from clifpy.tables import Patient, Hospitalization, Adt
    return Adt, Hospitalization, Path, Patient, json, pd


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

    # Create both output directories
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PHI_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Site: {SITE_NAME}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Filetype: {FILETYPE}")
    print(f"Timezone: {TIMEZONE}")
    return DATA_DIR, FILETYPE, OUTPUT_DIR, PHI_DIR, SITE_NAME, TIMEZONE


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
    ## Step 2: Apply Inclusion Criteria
    """)
    return


@app.cell
def _(adt, hospitalization, pd):
    # Get dataframes
    hosp_df = hospitalization.df.copy()
    adt_df = adt.df.copy()

    # Merge hospitalization and ADT to get hospital_type
    merged_df = pd.merge(
        hosp_df,
        adt_df[["hospitalization_id", "hospital_id", "hospital_type", "in_dttm", "location_category"]].drop_duplicates(),
        on="hospitalization_id",
        how="inner"
    )

    print(f"Total hospitalizations after merge: {merged_df['hospitalization_id'].nunique():,}")
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
        cohort_df_filtered["admission_type_category"] == "ed"
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
def _(adt, cohort_df_hospital):
    # Filter 5: Must have ED location during hospitalization
    hosp_with_ed = set(adt.df[adt.df["location_category"] == "ed"]["hospitalization_id"].unique())

    cohort_df_final = cohort_df_hospital[
        cohort_df_hospital["hospitalization_id"].isin(hosp_with_ed)
    ].copy()

    print(f"After ED location filter: {cohort_df_final['hospitalization_id'].nunique():,} hospitalizations")
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
        race = row["race_category"]
        eth = row["ethnicity_category"]
        if race == "Asian":
            return "Asian"
        elif eth == "Hispanic" and race == "Black or African American":
            return "Hispanic Black"
        elif eth == "Hispanic" and race == "White":
            return "Hispanic White"
        elif eth == "Non-Hispanic" and race == "Black or African American":
            return "Non-Hispanic Black"
        elif eth == "Non-Hispanic" and race == "White":
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
def _(PHI_DIR, SITE_NAME, final_cohort):
    # Save cohort (PHI - patient-level data)
    cohort_output_path = PHI_DIR / f"{SITE_NAME}_cohort_df.parquet"
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
    ## Step 5: Calculate Adult Sepsis Event (ASE)
    """)
    return


@app.cell
def _():
    from clifpy.utils.ase import compute_ase
    return (compute_ase,)


@app.cell
def _(compute_ase, final_cohort, pd):
    # Get hospitalization IDs from cohort
    hosp_ids = final_cohort["hospitalization_id"].astype(str).unique().tolist()
    print(f"Running ASE calculation on {len(hosp_ids):,} hospitalizations...")

    # Calculate ASE using clifpy (returns ALL blood cultures, both ASE and non-ASE)
    # include_lactate=True is REQUIRED to compare lactate vs non-lactate definitions
    # - sepsis: ASE with lactate as organ dysfunction criterion
    # - sepsis_wo_lactate: ASE without lactate criterion
    ase_results_all = compute_ase(
        hospitalization_ids=hosp_ids,
        config_path="clif_config.json",
        apply_rit=True,
        rit_only_hospital_onset=True,
        include_lactate=True,  # Must be True to compare lactate vs non-lactate
        verbose=True
    )

    # Keep first ASE episode OR first blood culture (for non-ASE patients)
    # For ASE patients: episode_id == 1 (first ASE after RIT)
    # For non-ASE patients: first blood culture by date (episode_id is NA)
    ase_first = ase_results_all[ase_results_all["episode_id"] == 1].copy()
    non_ase_first = (
        ase_results_all[ase_results_all["episode_id"].isna()]
        .sort_values("blood_culture_dttm")
        .drop_duplicates(subset=["hospitalization_id"], keep="first")
    )
    ase_results = pd.concat([ase_first, non_ase_first], ignore_index=True)

    print(f"Filtered to first episode/BC: {len(ase_results):,} hospitalizations")
    print(f"  - ASE cases: {ase_first['hospitalization_id'].nunique():,}")
    print(f"  - Non-ASE with blood culture: {non_ase_first['hospitalization_id'].nunique():,}")
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
def _(PHI_DIR, SITE_NAME, ase_results):
    # Save ASE results (PHI - patient-level data)
    ase_output_path = PHI_DIR / f"{SITE_NAME}_ase_results.parquet"
    ase_results.to_parquet(ase_output_path, index=False)

    print(f"ASE results saved to: {ase_output_path}")
    print(f"Shape: {ase_results.shape}")
    return


@app.cell
def _(ase_results, pd, OUTPUT_DIR, SITE_NAME):
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

    # Group 1: ASE with lactate criterion AND actually had elevated lactate
    group1 = ase_results[(ase_results['sepsis'] == 1) & (ase_results['lactate_dttm'].notna())]
    # Group 2: All ASE (6-criterion definition)
    group2 = ase_results[ase_results['sepsis'] == 1]
    # Group 3: ASE without lactate criterion (5-criterion definition)
    group3 = ase_results[ase_results['sepsis_wo_lactate'] == 1]

    groups = {
        'ASE_with_lactate_AND_met_lactate': group1,
        'ASE_with_lactate': group2,
        'ASE_without_lactate': group3,
    }

    rows = {}
    for gname, gdf in groups.items():
        n_total = len(gdf)
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

    out_path = OUTPUT_DIR / f"{SITE_NAME}_organ_dysfunction_breakdown.csv"
    breakdown_df.to_csv(out_path)
    print(f"Organ dysfunction breakdown saved to: {out_path}")
    print(f"Shape: {breakdown_df.shape}")
    print(breakdown_df.to_string())
    return


@app.cell
def _(ase_results):
    # Display ASE results
    ase_results
    return


if __name__ == "__main__":
    app.run()
